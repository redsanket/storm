;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.daemon.nimbus
  (:import [org.apache.thrift.server THsHaServer THsHaServer$Args])
  (:import [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift.exception])
  (:import [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [org.apache.commons.io FileUtils])
  (:import [javax.security.auth Subject])
  (:import [java.nio ByteBuffer]
           [java.util Collections HashMap]
           [backtype.storm.generated NimbusSummary])
  (:import [backtype.storm.security.auth NimbusPrincipal])
  (:import [java.util Iterator])
  (:import [java.nio ByteBuffer]
           [java.util Collections List HashMap])
  (:import [backtype.storm.blobstore AtomicOutputStream
            BlobStore
            BlobStoreAclHandler
            ClientBlobStore
            InputStreamWithMeta])
            (:import [java.io FileNotFoundException File FileOutputStream FileInputStream])
  (:import [java.net InetAddress])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:import [backtype.storm.security.auth ThriftServer ThriftConnectionType ReqContext AuthUtils])
  (:use [backtype.storm.scheduler.DefaultScheduler])
  (:import [backtype.storm.scheduler INimbus SupervisorDetails WorkerSlot TopologyDetails
            Cluster Topologies SchedulerAssignment SchedulerAssignmentImpl DefaultScheduler ExecutorDetails])
  (:import [backtype.storm.nimbus NimbusInfo])
  (:import [backtype.storm.utils TimeCacheMap TimeCacheMap$ExpiredCallback Utils ThriftTopologyUtils
            BufferFileInputStream BufferInputStream])
  (:import [backtype.storm.generated NotAliveException AlreadyAliveException StormTopology ErrorInfo
            ExecutorInfo InvalidTopologyException Nimbus$Iface Nimbus$Processor SubmitOptions TopologyInitialStatus
            KillOptions RebalanceOptions ClusterSummary SupervisorSummary TopologySummary TopologyInfo
            ExecutorSummary AuthorizationException GetInfoOptions NumErrorsChoice SettableBlobMeta ReadableBlobMeta
            BeginDownloadResult ListBlobsResult BlobReplication])

  (:import [backtype.storm.daemon Shutdownable])
  (:use [backtype.storm util config log timer zookeeper])
  (:require [backtype.storm [cluster :as cluster] [stats :as stats] [converter :as converter]])
  (:require [clojure.set :as set])
  (:import [backtype.storm.daemon.common StormBase Assignment])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm config])
  (:import [org.apache.zookeeper data.ACL ZooDefs$Ids ZooDefs$Perms])
  (:import [backtype.storm.utils VersionInfo])
  (:gen-class
    :methods [^{:static true} [launch [backtype.storm.scheduler.INimbus] void]]))

(defn file-cache-map [conf]
  (TimeCacheMap.
   (int (conf NIMBUS-FILE-COPY-EXPIRATION-SECS))
   (reify TimeCacheMap$ExpiredCallback
          (expire [this id stream]
                  (.close stream)
                  ))
   ))

(defn mk-scheduler [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)
        scheduler (cond
                    forced-scheduler
                    (do (log-message "Using forced scheduler from INimbus " (class forced-scheduler))
                        forced-scheduler)
    
                    (conf STORM-SCHEDULER)
                    (do (log-message "Using custom scheduler: " (conf STORM-SCHEDULER))
                        (-> (conf STORM-SCHEDULER) new-instance))
    
                    :else
                    (do (log-message "Using default scheduler")
                        (DefaultScheduler.)))]
    (.prepare scheduler conf)
    scheduler
    ))

(defmulti mk-code-distributor cluster-mode)
(defmulti sync-code cluster-mode)

(defnk is-leader [nimbus :throw-exception true]
  (let [leader-elector (:leader-elector nimbus)]
    (if (.isLeader leader-elector) true
      (if throw-exception
        (let [leader-address (.getLeader leader-elector)]
          (throw (RuntimeException. (str "not a leader, current leader is " leader-address))))))))

(def NIMBUS-ZK-ACLS
  [(first ZooDefs$Ids/CREATOR_ALL_ACL) 
   (ACL. (bit-or ZooDefs$Perms/READ ZooDefs$Perms/CREATE) ZooDefs$Ids/ANYONE_ID_UNSAFE)])

(defn mk-blob-cache-map
  "Constructs a TimeCacheMap instance with a blob store timeout whose
  expiration callback invokes cancel on the value held by an expired entry when
  that value is an AtomicOutputStream and calls close otherwise."
  [conf]
  (TimeCacheMap.
    (int (conf NIMBUS-BLOBSTORE-EXPIRATION-SECS))
    (reify TimeCacheMap$ExpiredCallback
      (expire [this id stream]
        (if (instance? AtomicOutputStream stream)
          (.cancel stream)
          (.close stream))))))

(defn mk-bloblist-cache-map
  "Constructs a TimeCacheMap instance with a blobstore timeout and no callback
  function."
  [conf]
  (TimeCacheMap. (int (conf NIMBUS-BLOBSTORE-EXPIRATION-SECS))))

(defn nimbus-data [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)]
    {:conf conf
     :nimbus-host-port-info (NimbusInfo/fromConf conf)
     :inimbus inimbus
     :authorization-handler (mk-authorization-handler (conf NIMBUS-AUTHORIZER) conf)
     :impersonation-authorization-handler (mk-authorization-handler (conf NIMBUS-IMPERSONATION-AUTHORIZER) conf)
     :submitted-count (atom 0)
     :storm-cluster-state (cluster/mk-storm-cluster-state conf :acls (when
                                                                       (Utils/isZkAuthenticationConfiguredStormServer
                                                                         conf)
                                                                       NIMBUS-ZK-ACLS))
     :submit-lock (Object.)
     :cred-update-lock (Object.)
     :heartbeats-cache (atom {})
     :downloaders (file-cache-map conf)
     :uploaders (file-cache-map conf)
     :blob-store (Utils/getNimbusBlobStore conf)
     :blob-downloaders (mk-blob-cache-map conf)
     :blob-uploaders (mk-blob-cache-map conf)
     :blob-listers (mk-bloblist-cache-map conf)
     :uptime (uptime-computer)
     :validator (new-instance (conf NIMBUS-TOPOLOGY-VALIDATOR))
     :timer (mk-timer :kill-fn (fn [t]
                                 (log-error t "Error when processing event")
                                 (exit-process! 20 "Error when processing an event")
                                 ))
     :scheduler (mk-scheduler conf inimbus)
     :leader-elector (zk-leader-elector conf)
     :code-distributor (mk-code-distributor conf)
     :id->sched-status (atom {})
     :cred-renewers (AuthUtils/GetCredentialRenewers conf)
     :nimbus-autocred-plugins (AuthUtils/getNimbusAutoCredPlugins conf)
     }))

(defn inbox [nimbus]
  (master-inbox (:conf nimbus)))

(defn- get-subject []
  (let [req (ReqContext/context)]
    (.subject req)))

(defn- read-storm-conf [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (merge conf
       (clojurify-structure
         (Utils/fromCompressedJsonConf
           (FileUtils/readFileToByteArray
             (File. (master-stormconf-path stormroot))))))))

(declare delay-event)
(declare mk-assignments)

(defn kill-transition [nimbus storm-id]
  (fn [kill-time]
    (let [delay (if kill-time
                  kill-time
                  (get (read-storm-conf (:conf nimbus) storm-id)
                       TOPOLOGY-MESSAGE-TIMEOUT-SECS))]
      (delay-event nimbus
                   storm-id
                   delay
                   :remove)
      {
        :status {:type :killed}
        :topology-action-options {:delay-secs delay :action :kill}})
    ))

(defn rebalance-transition [nimbus storm-id status]
  (fn [time num-workers executor-overrides]
    (let [delay (if time
                  time
                  (get (read-storm-conf (:conf nimbus) storm-id)
                       TOPOLOGY-MESSAGE-TIMEOUT-SECS))]
      (delay-event nimbus
                   storm-id
                   delay
                   :do-rebalance)
      {:status {:type :rebalancing}
       :prev-status status
       :topology-action-options (-> {:delay-secs delay :action :rebalance}
                                  (assoc-non-nil :num-workers num-workers)
                                  (assoc-non-nil :component->executors executor-overrides))
       })))

(defn do-rebalance [nimbus storm-id status storm-base]
  (let [rebalance-options (:topology-action-options storm-base)]
    (.update-storm! (:storm-cluster-state nimbus)
      storm-id
        (-> {:topology-action-options nil}
          (assoc-non-nil :component->executors (:component->executors rebalance-options))
          (assoc-non-nil :num-workers (:num-workers rebalance-options)))))
  (mk-assignments nimbus :scratch-topology-id storm-id))

(defn state-transitions [nimbus storm-id status storm-base]
  {:active {:inactivate :inactive
            :activate nil
            :rebalance (rebalance-transition nimbus storm-id status)
            :kill (kill-transition nimbus storm-id)
            }
   :inactive {:activate :active
              :inactivate nil
              :rebalance (rebalance-transition nimbus storm-id status)
              :kill (kill-transition nimbus storm-id)
              }
   :killed {:startup (fn [] (delay-event nimbus
                                         storm-id
                                         (-> storm-base
                                             :topology-action-options
                                             :delay-secs)
                                         :remove)
                             nil)
            :kill (kill-transition nimbus storm-id)
            :remove (fn []
                      (log-message "Killing topology: " storm-id)
                      (.remove-storm! (:storm-cluster-state nimbus)
                                      storm-id)
                      nil)
            }
   :rebalancing {:startup (fn [] (delay-event nimbus
                                              storm-id
                                              (-> storm-base
                                                  :topology-action-options
                                                  :delay-secs)
                                              :do-rebalance)
                                 nil)
                 :kill (kill-transition nimbus storm-id)
                 :do-rebalance (fn []
                                 (do-rebalance nimbus storm-id status storm-base)
                                 (:type (:prev-status storm-base)))
                 }})

(defn transition!
  ([nimbus storm-id event]
     (transition! nimbus storm-id event false))
  ([nimbus storm-id event error-on-no-transition?]
    (is-leader nimbus)
    (locking (:submit-lock nimbus)
       (let [system-events #{:startup}
             [event & event-args] (if (keyword? event) [event] event)
             storm-base (-> nimbus :storm-cluster-state  (.storm-base storm-id nil))
             status (:status storm-base)]
         ;; handles the case where event was scheduled but topology has been removed
         (if-not status
           (log-message "Cannot apply event " event " to " storm-id " because topology no longer exists")
           (let [get-event (fn [m e]
                             (if (contains? m e)
                               (m e)
                               (let [msg (str "No transition for event: " event
                                              ", status: " status,
                                              " storm-id: " storm-id)]
                                 (if error-on-no-transition?
                                   (throw-runtime msg)
                                   (do (when-not (contains? system-events event)
                                         (log-message msg))
                                       nil))
                                 )))
                 transition (-> (state-transitions nimbus storm-id status storm-base)
                                (get (:type status))
                                (get-event event))
                 transition (if (or (nil? transition)
                                    (keyword? transition))
                              (fn [] transition)
                              transition)
                 storm-base-updates (apply transition event-args)
                 storm-base-updates (if (keyword? storm-base-updates) ;if it's just a symbol, that just indicates new status.
                                      {:status {:type storm-base-updates}}
                                      storm-base-updates)]

             (when storm-base-updates
               (.update-storm! (:storm-cluster-state nimbus) storm-id storm-base-updates)))))
       )))

(defn transition-name! [nimbus storm-name event & args]
  (let [storm-id (get-storm-id (:storm-cluster-state nimbus) storm-name)]
    (when-not storm-id
      (throw (NotAliveException. storm-name)))
    (apply transition! nimbus storm-id event args)))

(defn delay-event [nimbus storm-id delay-secs event]
  (log-message "Delaying event " event " for " delay-secs " secs for " storm-id)
  (schedule (:timer nimbus)
            delay-secs
            #(transition! nimbus storm-id event false)
            ))

;; active -> reassign in X secs

;; killed -> wait kill time then shutdown
;; active -> reassign in X secs
;; inactive -> nothing
;; rebalance -> wait X seconds then rebalance
;; swap... (need to handle kill during swap, etc.)
;; event transitions are delayed by timer... anything else that comes through (e.g. a kill) override the transition? or just disable other transitions during the transition?


(defmulti setup-jar cluster-mode)
(defmulti clean-inbox cluster-mode)

;; swapping design
;; -- need 2 ports per worker (swap port and regular port)
;; -- topology that swaps in can use all the existing topologies swap ports, + unused worker slots
;; -- how to define worker resources? port range + number of workers?


;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;; 1. read assignment
;; 2. see which executors/nodes are up
;; 3. make new assignment to fix any problems
;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove executors and finally remove assignments)

(defn- assigned-slots
  "Returns a map from node-id to a set of ports"
  [storm-cluster-state]

  (let [assignments (.assignments storm-cluster-state nil)]
    (defaulted
      (apply merge-with set/union
             (for [a assignments
                   [_ [node port]] (-> (.assignment-info storm-cluster-state a nil) :executor->node+port)]
               {node #{port}}
               ))
      {})
    ))

(defn- all-supervisor-info
  ([storm-cluster-state] (all-supervisor-info storm-cluster-state nil))
  ([storm-cluster-state callback]
     (let [supervisor-ids (.supervisors storm-cluster-state callback)]
       (into {}
             (mapcat
              (fn [id]
                (if-let [info (.supervisor-info storm-cluster-state id)]
                  [[id info]]
                  ))
              supervisor-ids))
       )))

(defn- all-scheduling-slots
  [nimbus topologies missing-assignment-topologies]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        ^INimbus inimbus (:inimbus nimbus)

        supervisor-infos (all-supervisor-info storm-cluster-state nil)

        supervisor-details (dofor [[id info] supervisor-infos]
                             (SupervisorDetails. id (:meta info)))

        ret (.allSlotsAvailableForScheduling inimbus
                     supervisor-details
                     topologies
                     (set missing-assignment-topologies)
                     )
        ]
    (dofor [^WorkerSlot slot ret]
      [(.getNodeId slot) (.getPort slot)]
      )))

;(defn- setup-storm-code [nimbus conf storm-id tmp-jar-location storm-conf topology]
;  (let [stormroot (master-stormdist-root conf storm-id)]
;   (log-message "nimbus file location:" stormroot)
;   (FileUtils/forceMkdir (File. stormroot))
;   (FileUtils/cleanDirectory (File. stormroot))
;   (setup-jar conf tmp-jar-location stormroot)
;   (FileUtils/writeByteArrayToFile (File. (master-stormcode-path stormroot)) (Utils/serialize topology))
;   (FileUtils/writeByteArrayToFile (File. (master-stormconf-path stormroot)) (Utils/toCompressedJsonConf storm-conf))
;   (if (:code-distributor nimbus) (.upload (:code-distributor nimbus) stormroot storm-id))
;   ))
(defn- get-metadata-version [blob-store key subject]
  (-> blob-store
    .getBlobMeta key subject
    .get_version))

(defn- setup-storm-code [nimbus conf storm-id tmp-jar-location storm-conf topology]
  (let [subject (get-subject)
        storm-cluster-state (:storm-cluster-state nimbus)
        blob-store (:blob-store nimbus)
        jar-key (master-stormjar-key storm-id)
        code-key (master-stormcode-key storm-id)
        conf-key (master-stormconf-key storm-id)
        nimbus-host-port-info (:nimbus-host-port-info nimbus)]
    (if tmp-jar-location ;;in local mode there is no jar
      (do
        (.createBlob blob-store jar-key (FileInputStream. tmp-jar-location) (SettableBlobMeta. BlobStoreAclHandler/DEFAULT) subject)
        (.setup-blobstore! storm-cluster-state jar-key nimbus-host-port-info (get-metadata-version blob-store jar-key subject) (str "active"))))
    (.createBlob blob-store code-key (Utils/serialize topology) (SettableBlobMeta. BlobStoreAclHandler/DEFAULT) subject)
    (.createBlob blob-store conf-key (Utils/toCompressedJsonConf storm-conf) (SettableBlobMeta. BlobStoreAclHandler/DEFAULT) subject)
    (.setup-blobstore! storm-cluster-state code-key nimbus-host-port-info (get-metadata-version blob-store code-key subject) (str "active"))
    (.setup-blobstore! storm-cluster-state conf-key nimbus-host-port-info (get-metadata-version blob-store conf-key subject) (str "active"))))

(defn- read-storm-topology [storm-id blob-store]
  (Utils/deserialize
    (.readBlob blob-store (master-stormcode-key storm-id) (get-subject)) StormTopology))

(defn- get-nimbus-subject []
  (let [nimbus-subject (Subject.)
        nimbus-principal (NimbusPrincipal.)
        principals (.getPrincipals nimbus-subject)]
    (.add principals nimbus-principal)
    nimbus-subject))

(defn- wait-for-desired-code-replication [nimbus conf storm-id]
  (let [min-replication-count (conf TOPOLOGY-MIN-REPLICATION-COUNT)
        max-replication-wait-time (conf TOPOLOGY-MAX-REPLICATION-WAIT-TIME-SEC)
        total-wait-time (atom 0)
        current-replication-count (atom (if (:code-distributor nimbus) (.getReplicationCount (:code-distributor nimbus) storm-id) 0))]
  (if (:code-distributor nimbus)
    (while (and (> min-replication-count @current-replication-count)
             (or (= -1 max-replication-wait-time)
               (< @total-wait-time max-replication-wait-time)))
        (sleep-secs 1)
        (log-debug "waiting for desired replication to be achieved.
          min-replication-count = " min-replication-count  " max-replication-wait-time = " max-replication-wait-time
          "current-replication-count = " @current-replication-count " total-wait-time " @total-wait-time)
        (swap! total-wait-time inc)
        (reset! current-replication-count  (.getReplicationCount (:code-distributor nimbus) storm-id))))
  (if (< min-replication-count @current-replication-count)
    (log-message "desired replication count "  min-replication-count " achieved,
      current-replication-count" @current-replication-count)
    (log-message "desired replication count of "  min-replication-count " not achieved but we have hit the max wait time "
      max-replication-wait-time " so moving on with replication count = " @current-replication-count)
    )))

(defn- read-storm-topology [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (Utils/deserialize
      (FileUtils/readFileToByteArray
        (File. (master-stormcode-path stormroot))
        ) StormTopology)))

(defn- read-storm-topology-as-nimbus [storm-id blob-store]
  (Utils/deserialize
    (.readBlob blob-store (master-stormcode-key storm-id) (get-nimbus-subject)) StormTopology))

(declare compute-executor->component)

(defn- get-nimbus-subject []
  (let [nimbus-subject (Subject.)
        nimbus-principal (NimbusPrincipal.)
        principals (.getPrincipals nimbus-subject)]
    (.add principals nimbus-principal)
    nimbus-subject))

(defn read-storm-conf-as-nimbus [conf storm-id blob-store]
  (clojurify-structure
    (Utils/fromCompressedJsonConf
      (.readBlob blob-store (master-stormconf-key storm-id) (get-nimbus-subject)))))

(defn read-topology-details [nimbus storm-id]
  (let [conf (:conf nimbus)
        storm-base (.storm-base (:storm-cluster-state nimbus) storm-id nil)
        topology-conf (read-storm-conf-as-nimbus conf storm-id)
        topology (read-storm-topology-as-nimbus conf storm-id)
        executor->component (->> (compute-executor->component nimbus storm-id)
                                 (map-key (fn [[start-task end-task]]
                                            (ExecutorDetails. (int start-task) (int end-task)))))]
    (TopologyDetails. storm-id
                      topology-conf
                      topology
                      (:num-workers storm-base)
                      executor->component
                      )))

;; Does not assume that clocks are synchronized. Executor heartbeat is only used so that
;; nimbus knows when it's received a new heartbeat. All timing is done by nimbus and
;; tracked through heartbeat-cache
(defn- update-executor-cache [curr hb timeout]
  (let [reported-time (:time-secs hb)
        {last-nimbus-time :nimbus-time
         last-reported-time :executor-reported-time} curr
        reported-time (cond reported-time reported-time
                            last-reported-time last-reported-time
                            :else 0)
        nimbus-time (if (or (not last-nimbus-time)
                        (not= last-reported-time reported-time))
                      (current-time-secs)
                      last-nimbus-time
                      )]
      {:is-timed-out (and
                       nimbus-time
                       (>= (time-delta nimbus-time) timeout))
       :nimbus-time nimbus-time
       :executor-reported-time reported-time
       :heartbeat hb}))

(defn update-heartbeat-cache [cache executor-beats all-executors timeout]
  (let [cache (select-keys cache all-executors)]
    (into {}
      (for [executor all-executors :let [curr (cache executor)]]
        [executor
         (update-executor-cache curr (get executor-beats executor) timeout)]
         ))))

(defn update-heartbeats! [nimbus storm-id all-executors existing-assignment]
  (log-debug "Updating heartbeats for " storm-id " " (pr-str all-executors))
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        executor-beats (.executor-beats storm-cluster-state storm-id (:executor->node+port existing-assignment))
        cache (update-heartbeat-cache (@(:heartbeats-cache nimbus) storm-id)
                                      executor-beats
                                      all-executors
                                      ((:conf nimbus) NIMBUS-TASK-TIMEOUT-SECS))]
      (swap! (:heartbeats-cache nimbus) assoc storm-id cache)))

(defn- update-all-heartbeats! [nimbus existing-assignments topology->executors]
  "update all the heartbeats for all the topologies's executors"
  (doseq [[tid assignment] existing-assignments
          :let [all-executors (topology->executors tid)]]
    (update-heartbeats! nimbus tid all-executors assignment)))

(defn- alive-executors
  [nimbus ^TopologyDetails topology-details all-executors existing-assignment]
  (log-debug "Computing alive executors for " (.getId topology-details) "\n"
             "Executors: " (pr-str all-executors) "\n"
             "Assignment: " (pr-str existing-assignment) "\n"
             "Heartbeat cache: " (pr-str (@(:heartbeats-cache nimbus) (.getId topology-details)))
             )
  ;; TODO: need to consider all executors associated with a dead executor (in same slot) dead as well,
  ;; don't just rely on heartbeat being the same
  (let [conf (:conf nimbus)
        storm-id (.getId topology-details)
        executor-start-times (:executor->start-time-secs existing-assignment)
        heartbeats-cache (@(:heartbeats-cache nimbus) storm-id)]
    (->> all-executors
        (filter (fn [executor]
          (let [start-time (get executor-start-times executor)
                is-timed-out (-> heartbeats-cache (get executor) :is-timed-out)]
            (if (and start-time
                   (or
                    (< (time-delta start-time)
                       (conf NIMBUS-TASK-LAUNCH-SECS))
                    (not is-timed-out)
                    ))
              true
              (do
                (log-message "Executor " storm-id ":" executor " not alive")
                false))
            )))
        doall)))


(defn- to-executor-id [task-ids]
  [(first task-ids) (last task-ids)])

(defn- compute-executors [nimbus storm-id]
  (let [conf (:conf nimbus)
        storm-base (.storm-base (:storm-cluster-state nimbus) storm-id nil)
        component->executors (:component->executors storm-base)
        storm-conf (read-storm-conf-as-nimbus conf storm-id)
        topology (read-storm-topology-as-nimbus conf storm-id)
        task->component (storm-task-info topology storm-conf)]
    (->> (storm-task-info topology storm-conf)
         reverse-map
         (map-val sort)
         (join-maps component->executors)
         (map-val (partial apply partition-fixed))
         (mapcat second)
         (map to-executor-id)
         )))

(defn- compute-executor->component [nimbus storm-id]
  (let [conf (:conf nimbus)
        executors (compute-executors nimbus storm-id)
        topology (read-storm-topology-as-nimbus conf storm-id)
        storm-conf (read-storm-conf-as-nimbus conf storm-id)
        task->component (storm-task-info topology storm-conf)
        executor->component (into {} (for [executor executors
                                           :let [start-task (first executor)
                                                 component (task->component start-task)]]
                                       {executor component}))]
        executor->component))

(defn- compute-topology->executors [nimbus storm-ids]
  "compute a topology-id -> executors map"
  (into {} (for [tid storm-ids]
             {tid (set (compute-executors nimbus tid))})))

(defn- compute-topology->alive-executors [nimbus existing-assignments topologies topology->executors scratch-topology-id]
  "compute a topology-id -> alive executors map"
  (into {} (for [[tid assignment] existing-assignments
                 :let [topology-details (.getById topologies tid)
                       all-executors (topology->executors tid)
                       alive-executors (if (and scratch-topology-id (= scratch-topology-id tid))
                                         all-executors
                                         (set (alive-executors nimbus topology-details all-executors assignment)))]]
             {tid alive-executors})))

(defn- compute-supervisor->dead-ports [nimbus existing-assignments topology->executors topology->alive-executors]
  (let [dead-slots (into [] (for [[tid assignment] existing-assignments
                                  :let [all-executors (topology->executors tid)
                                        alive-executors (topology->alive-executors tid)
                                        dead-executors (set/difference all-executors alive-executors)
                                        dead-slots (->> (:executor->node+port assignment)
                                                        (filter #(contains? dead-executors (first %)))
                                                        vals)]]
                              dead-slots))
        supervisor->dead-ports (->> dead-slots
                                    (apply concat)
                                    (map (fn [[sid port]] {sid #{port}}))
                                    (apply (partial merge-with set/union)))]
    (or supervisor->dead-ports {})))

(defn- compute-topology->scheduler-assignment [nimbus existing-assignments topology->alive-executors]
  "convert assignment information in zk to SchedulerAssignment, so it can be used by scheduler api."
  (into {} (for [[tid assignment] existing-assignments
                 :let [alive-executors (topology->alive-executors tid)
                       executor->node+port (:executor->node+port assignment)
                       executor->slot (into {} (for [[executor [node port]] executor->node+port]
                                                 ;; filter out the dead executors
                                                 (if (contains? alive-executors executor)
                                                   {(ExecutorDetails. (first executor)
                                                                      (second executor))
                                                    (WorkerSlot. node port)}
                                                   {})))]]
             {tid (SchedulerAssignmentImpl. tid executor->slot)})))

(defn- read-all-supervisor-details [nimbus all-scheduling-slots supervisor->dead-ports]
  "return a map: {supervisor-id SupervisorDetails}"
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        supervisor-infos (all-supervisor-info storm-cluster-state)
        nonexistent-supervisor-slots (apply dissoc all-scheduling-slots (keys supervisor-infos))
        all-supervisor-details (into {} (for [[sid supervisor-info] supervisor-infos
                                              :let [hostname (:hostname supervisor-info)
                                                    scheduler-meta (:scheduler-meta supervisor-info)
                                                    dead-ports (supervisor->dead-ports sid)
                                                    ;; hide the dead-ports from the all-ports
                                                    ;; these dead-ports can be reused in next round of assignments
                                                    all-ports (-> (get all-scheduling-slots sid)
                                                                  (set/difference dead-ports)
                                                                  ((fn [ports] (map int ports))))
                                                    supervisor-details (SupervisorDetails. sid hostname scheduler-meta all-ports)]]
                                          {sid supervisor-details}))]
    (merge all-supervisor-details
           (into {}
              (for [[sid ports] nonexistent-supervisor-slots]
                [sid (SupervisorDetails. sid nil ports)]))
           )))

(defn- compute-topology->executor->node+port [scheduler-assignments]
  "convert {topology-id -> SchedulerAssignment} to
           {topology-id -> {executor [node port]}}"
  (map-val (fn [^SchedulerAssignment assignment]
             (->> assignment
                  .getExecutorToSlot
                  (#(into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] %]
                              {[(.getStartTask executor) (.getEndTask executor)]
                               [(.getNodeId slot) (.getPort slot)]})))))
           scheduler-assignments))

;; NEW NOTES
;; only assign to supervisors who are there and haven't timed out
;; need to reassign workers with executors that have timed out (will this make it brittle?)
;; need to read in the topology and storm-conf from disk
;; if no slots available and no slots used by this storm, just skip and do nothing
;; otherwise, package rest of executors into available slots (up to how much it needs)

;; in the future could allocate executors intelligently (so that "close" tasks reside on same machine)

;; TODO: slots that have dead executor should be reused as long as supervisor is active


;; (defn- assigned-slots-from-scheduler-assignments [topology->assignment]
;;   (->> topology->assignment
;;        vals
;;        (map (fn [^SchedulerAssignment a] (.getExecutorToSlot a)))
;;        (mapcat vals)
;;        (map (fn [^WorkerSlot s] {(.getNodeId s) #{(.getPort s)}}))
;;        (apply merge-with set/union)
;;        ))

(defn num-used-workers [^SchedulerAssignment scheduler-assignment]
  (if scheduler-assignment
    (count (.getSlots scheduler-assignment))
    0 ))

;; public so it can be mocked out
(defn compute-new-topology->executor->node+port [nimbus existing-assignments topologies scratch-topology-id]
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)
        topology->executors (compute-topology->executors nimbus (keys existing-assignments))
        ;; update the executors heartbeats first.
        _ (update-all-heartbeats! nimbus existing-assignments topology->executors)
        topology->alive-executors (compute-topology->alive-executors nimbus
                                                                     existing-assignments
                                                                     topologies
                                                                     topology->executors
                                                                     scratch-topology-id)
        supervisor->dead-ports (compute-supervisor->dead-ports nimbus
                                                               existing-assignments
                                                               topology->executors
                                                               topology->alive-executors)
        topology->scheduler-assignment (compute-topology->scheduler-assignment nimbus
                                                                               existing-assignments
                                                                               topology->alive-executors)

        missing-assignment-topologies (->> topologies
                                           .getTopologies
                                           (map (memfn getId))
                                           (filter (fn [t]
                                                      (let [alle (get topology->executors t)
                                                            alivee (get topology->alive-executors t)]
                                                            (or (empty? alle)
                                                                (not= alle alivee)
                                                                (< (-> topology->scheduler-assignment
                                                                       (get t)
                                                                       num-used-workers )
                                                                   (-> topologies (.getById t) .getNumWorkers)
                                                                   ))
                                                            ))))
        all-scheduling-slots (->> (all-scheduling-slots nimbus topologies missing-assignment-topologies)
                                  (map (fn [[node-id port]] {node-id #{port}}))
                                  (apply merge-with set/union))

        supervisors (read-all-supervisor-details nimbus all-scheduling-slots supervisor->dead-ports)
        cluster (Cluster. (:inimbus nimbus) supervisors topology->scheduler-assignment)

        ;; call scheduler.schedule to schedule all the topologies
        ;; the new assignments for all the topologies are in the cluster object.
        _ (.schedule (:scheduler nimbus) topologies cluster)
        new-scheduler-assignments (.getAssignments cluster)
        ;; add more information to convert SchedulerAssignment to Assignment
        new-topology->executor->node+port (compute-topology->executor->node+port new-scheduler-assignments)]
    (reset! (:id->sched-status nimbus) (.getStatusMap cluster))
    ;; print some useful information.
    (doseq [[topology-id executor->node+port] new-topology->executor->node+port
            :let [old-executor->node+port (-> topology-id
                                          existing-assignments
                                          :executor->node+port)
                  reassignment (filter (fn [[executor node+port]]
                                         (and (contains? old-executor->node+port executor)
                                              (not (= node+port (old-executor->node+port executor)))))
                                       executor->node+port)]]
      (when-not (empty? reassignment)
        (let [new-slots-cnt (count (set (vals executor->node+port)))
              reassign-executors (keys reassignment)]
          (log-message "Reassigning " topology-id " to " new-slots-cnt " slots")
          (log-message "Reassign executors: " (vec reassign-executors)))))

    new-topology->executor->node+port))

(defn changed-executors [executor->node+port new-executor->node+port]
  (let [executor->node+port (if executor->node+port (sort executor->node+port) nil)
        new-executor->node+port (if new-executor->node+port (sort new-executor->node+port) nil)
        slot-assigned (reverse-map executor->node+port)
        new-slot-assigned (reverse-map new-executor->node+port)
        brand-new-slots (map-diff slot-assigned new-slot-assigned)]
    (apply concat (vals brand-new-slots))
    ))

(defn newly-added-slots [existing-assignment new-assignment]
  (let [old-slots (-> (:executor->node+port existing-assignment)
                      vals
                      set)
        new-slots (-> (:executor->node+port new-assignment)
                      vals
                      set)]
    (set/difference new-slots old-slots)))


(defn basic-supervisor-details-map [storm-cluster-state]
  (let [infos (all-supervisor-info storm-cluster-state)]
    (->> infos
         (map (fn [[id info]]
                 [id (SupervisorDetails. id (:hostname info) (:scheduler-meta info) nil)]))
         (into {}))))

(defn- to-worker-slot [[node port]]
  (WorkerSlot. node port))

;; get existing assignment (just the executor->node+port map) -> default to {}
;; filter out ones which have a executor timeout
;; figure out available slots on cluster. add to that the used valid slots to get total slots. figure out how many executors should be in each slot (e.g., 4, 4, 4, 5)
;; only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
;; edge case for slots with no executor timeout but with supervisor timeout... just treat these as valid slots that can be reassigned to. worst comes to worse the executor will timeout and won't assign here next time around
(defnk mk-assignments [nimbus :scratch-topology-id nil]
  (if (is-leader nimbus :throw-exception false)
    (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)
        ^INimbus inimbus (:inimbus nimbus)
        ;; read all the topologies
        topology-ids (.active-storms storm-cluster-state)
        topologies (into {} (for [tid topology-ids]
                              {tid (read-topology-details nimbus tid)}))
        topologies (Topologies. topologies)
        ;; read all the assignments
        assigned-topology-ids (.assignments storm-cluster-state nil)
        existing-assignments (into {} (for [tid assigned-topology-ids]
                                        ;; for the topology which wants rebalance (specified by the scratch-topology-id)
                                        ;; we exclude its assignment, meaning that all the slots occupied by its assignment
                                        ;; will be treated as free slot in the scheduler code.
                                        (when (or (nil? scratch-topology-id) (not= tid scratch-topology-id))
                                          {tid (.assignment-info storm-cluster-state tid nil)})))
        ;; make the new assignments for topologies
        topology->executor->node+port (compute-new-topology->executor->node+port
                                       nimbus
                                       existing-assignments
                                       topologies
                                       scratch-topology-id)

        topology->executor->node+port (merge (into {} (for [id assigned-topology-ids] {id nil})) topology->executor->node+port)

        now-secs (current-time-secs)

        basic-supervisor-details-map (basic-supervisor-details-map storm-cluster-state)

        ;; construct the final Assignments by adding start-times etc into it
        new-assignments (into {} (for [[topology-id executor->node+port] topology->executor->node+port
                                        :let [existing-assignment (get existing-assignments topology-id)
                                              all-nodes (->> executor->node+port vals (map first) set)
                                              node->host (->> all-nodes
                                                              (mapcat (fn [node]
                                                                        (if-let [host (.getHostName inimbus basic-supervisor-details-map node)]
                                                                          [[node host]]
                                                                          )))
                                                              (into {}))
                                              all-node->host (merge (:node->host existing-assignment) node->host)
                                              reassign-executors (changed-executors (:executor->node+port existing-assignment) executor->node+port)
                                              start-times (merge (:executor->start-time-secs existing-assignment)
                                                                (into {}
                                                                      (for [id reassign-executors]
                                                                        [id now-secs]
                                                                        )))]]
                                   {topology-id (Assignment.
                                                 (master-stormdist-root conf topology-id)
                                                 (select-keys all-node->host all-nodes)
                                                 executor->node+port
                                                 start-times)}))]

    ;; tasks figure out what tasks to talk to by looking at topology at runtime
    ;; only log/set when there's been a change to the assignment
    (doseq [[topology-id assignment] new-assignments
            :let [existing-assignment (get existing-assignments topology-id)
                  topology-details (.getById topologies topology-id)]]
      (if (= existing-assignment assignment)
        (log-debug "Assignment for " topology-id " hasn't changed")
        (do
          (log-message "Setting new assignment for topology id " topology-id ": " (pr-str assignment))
          (.set-assignment! storm-cluster-state topology-id assignment)
          )))
    (->> new-assignments
          (map (fn [[topology-id assignment]]
            (let [existing-assignment (get existing-assignments topology-id)]
              [topology-id (map to-worker-slot (newly-added-slots existing-assignment assignment))]
              )))
          (into {})
          (.assignSlots inimbus topologies)))
    (log-message "not a leader, skipping assignments")))

(defn- start-storm [nimbus storm-name storm-id topology-initial-status]
  {:pre [(#{:active :inactive} topology-initial-status)]}
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        conf (:conf nimbus)
        storm-conf (read-storm-conf conf storm-id)
        topology (system-topology! storm-conf (read-storm-topology conf storm-id))
        num-executors (->> (all-components topology) (map-val num-start-executors))]
    (log-message "Activating " storm-name ": " storm-id)
    (.activate-storm! storm-cluster-state
                      storm-id
                      (StormBase. storm-name
                                  (current-time-secs)
                                  {:type topology-initial-status}
                                  (storm-conf TOPOLOGY-WORKERS)
                                  num-executors
                                  (storm-conf TOPOLOGY-SUBMITTER-USER)
                                  nil
                                  nil
                                  {}))))

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set assignments
;; 3. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

(defn storm-active? [storm-cluster-state storm-name]
  (not-nil? (get-storm-id storm-cluster-state storm-name)))

(defn check-storm-active! [nimbus storm-name active?]
  (if (= (not active?)
         (storm-active? (:storm-cluster-state nimbus)
                        storm-name))
    (if active?
      (throw (NotAliveException. (str storm-name " is not alive")))
      (throw (AlreadyAliveException. (str storm-name " is already active"))))
    ))

(defn check-authorization! 
  ([nimbus storm-name storm-conf operation context]
     (let [aclHandler (:authorization-handler nimbus)
           impersonation-authorizer (:impersonation-authorization-handler nimbus)
           ctx (or context (ReqContext/context))
           check-conf (if storm-conf storm-conf (if storm-name {TOPOLOGY-NAME storm-name}))]
       (log-message "[req " (.requestID ctx) "] Access from: " (.remoteAddress ctx) " principal:" (.principal ctx) " op:" operation)

       (if (.isImpersonating ctx)
         (do
          (log-warn "principal: " (.realPrincipal ctx) " is trying to impersonate principal: " (.principal ctx))
          (if impersonation-authorizer
           (if-not (.permit impersonation-authorizer ctx operation check-conf)
             (throw (AuthorizationException. (str "principal " (.realPrincipal ctx) " is not authorized to impersonate
                        principal " (.principal ctx) " from host " (.remoteAddress ctx) " Please see SECURITY.MD to learn
                        how to configure impersonation acls."))))
           (log-warn "impersonation attempt but " NIMBUS-IMPERSONATION-AUTHORIZER " has no authorizer configured. potential
                      security risk, please see SECURITY.MD to learn how to configure impersonation authorizer."))))

       (if aclHandler
         (if-not (.permit aclHandler ctx operation check-conf)
           (throw (AuthorizationException. (str operation (if storm-name (str " on topology " storm-name)) " is not authorized")))
           ))))
  ([nimbus storm-name storm-conf operation]
     (check-authorization! nimbus storm-name storm-conf operation (ReqContext/context))))

(defn code-ids [conf]
  (-> conf
      master-stormdist-root
      read-dir-contents
      set
      ))

(defn cleanup-storm-ids [conf storm-cluster-state]
  (let [heartbeat-ids (set (.heartbeat-storms storm-cluster-state))
        error-ids (set (.error-topologies storm-cluster-state))
        code-ids (code-ids conf)
        assigned-ids (set (.active-storms storm-cluster-state))]
    (set/difference (set/union heartbeat-ids error-ids code-ids) assigned-ids)
    ))

(defn extract-status-str [base]
  (let [t (-> base :status :type)]
    (.toUpperCase (name t))
    ))

(defn mapify-serializations [sers]
  (->> sers
       (map (fn [e] (if (map? e) e {e nil})))
       (apply merge)
       ))

(defn- component-parallelism [storm-conf component]
  (let [storm-conf (merge storm-conf (component-conf component))
        num-tasks (or (storm-conf TOPOLOGY-TASKS) (num-start-executors component))
        max-parallelism (storm-conf TOPOLOGY-MAX-TASK-PARALLELISM)
        ]
    (if max-parallelism
      (min max-parallelism num-tasks)
      num-tasks)))

(defn normalize-topology [storm-conf ^StormTopology topology]
  (let [ret (.deepCopy topology)]
    (doseq [[_ component] (all-components ret)]
      (.set_json_conf
        (.get_common component)
        (->> {TOPOLOGY-TASKS (component-parallelism storm-conf component)}
             (merge (component-conf component))
             to-json )))
    ret ))

(defn normalize-conf [conf storm-conf ^StormTopology topology]
  ;; ensure that serializations are same for all tasks no matter what's on
  ;; the supervisors. this also allows you to declare the serializations as a sequence
  (let [component-confs (map
                         #(-> (ThriftTopologyUtils/getComponentCommon topology %)
                              .get_json_conf
                              from-json)
                         (ThriftTopologyUtils/getComponentIds topology))
        total-conf (merge conf storm-conf)

        get-merged-conf-val (fn [k merge-fn]
                              (merge-fn
                               (concat
                                (mapcat #(get % k) component-confs)
                                (or (get storm-conf k)
                                    (get conf k)))))]
    ;; topology level serialization registrations take priority
    ;; that way, if there's a conflict, a user can force which serialization to use
    ;; append component conf to storm-conf
    (merge storm-conf
           {TOPOLOGY-KRYO-DECORATORS (get-merged-conf-val TOPOLOGY-KRYO-DECORATORS distinct)
            TOPOLOGY-KRYO-REGISTER (get-merged-conf-val TOPOLOGY-KRYO-REGISTER mapify-serializations)
            TOPOLOGY-ACKER-EXECUTORS (total-conf TOPOLOGY-ACKER-EXECUTORS)
            TOPOLOGY-EVENTLOGGER-EXECUTORS (total-conf TOPOLOGY-EVENTLOGGER-EXECUTORS)
            TOPOLOGY-MAX-TASK-PARALLELISM (total-conf TOPOLOGY-MAX-TASK-PARALLELISM)})))

(defn do-cleanup [nimbus]
  (if (is-leader nimbus :throw-exception false)
    (let [storm-cluster-state (:storm-cluster-state nimbus)
          conf (:conf nimbus)
          submit-lock (:submit-lock nimbus)]
      (let [to-cleanup-ids (locking submit-lock
                             (cleanup-storm-ids conf storm-cluster-state))]
        (when-not (empty? to-cleanup-ids)
          (doseq [id to-cleanup-ids]
            (log-message "Cleaning up " id)
            (if (:code-distributor nimbus) (.cleanup (:code-distributor nimbus) id))
            (.teardown-heartbeats! storm-cluster-state id)
            (.teardown-topology-errors! storm-cluster-state id)
            (rmr (master-stormdist-root conf id))
            (swap! (:heartbeats-cache nimbus) dissoc id))
          )))
    (log-message "not a leader, skipping cleanup")))

(defn- file-older-than? [now seconds file]
  (<= (+ (.lastModified file) (to-millis seconds)) (to-millis now)))

(defn clean-inbox [dir-location seconds]
  "Deletes jar files in dir older than seconds."
  (let [now (current-time-secs)
        pred #(and (.isFile %) (file-older-than? now seconds %))
        files (filter pred (file-seq (File. dir-location)))]
    (doseq [f files]
      (if (.delete f)
        (log-message "Cleaning inbox ... deleted: " (.getName f))
        ;; This should never happen
        (log-error "Cleaning inbox ... error deleting: " (.getName f))
        ))))

(defn cleanup-corrupt-topologies! [nimbus]
  (if (is-leader nimbus :throw-exception false)
    (let [storm-cluster-state (:storm-cluster-state nimbus)
          code-ids (set (code-ids (:conf nimbus)))
          active-topologies (set (.active-storms storm-cluster-state))
          corrupt-topologies (set/difference active-topologies code-ids)]
      (doseq [corrupt corrupt-topologies]
        (log-message "Corrupt topology " corrupt " has state on zookeeper but doesn't have a local dir on Nimbus. Cleaning up...")
        (.remove-storm! storm-cluster-state corrupt)
        )))
  (log-message "not a leader, skipping cleanup-corrupt-topologies"))

;;setsup code distributor entries for all current topologies for which code is available locally.
(defn setup-code-distributor [nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        locally-available-storm-ids (set (code-ids (:conf nimbus)))
        active-topologies (set (.active-storms storm-cluster-state))
        locally-available-active-storm-ids (set/intersection locally-available-storm-ids active-topologies)]
    (doseq [storm-id locally-available-active-storm-ids]
      (.setup-code-distributor! storm-cluster-state storm-id (:nimbus-host-port-info nimbus)))))

(defn get-keys [blob-store]
  (let [key-iter (.listKeys blob-store)]
    (if (not-nil? (.hasNext key-iter))
      (let [ret-set (->> key-iter
                         (iterator-seq)
                         (java.util.ArrayList.))]
      ret-set))))

;;setsup blobstore for all current keys
(defn setup-blobstore [nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        blob-store (:blob-store nimbus)
        local-list-of-keys (set (get-keys blob-store))
        all-keys (set (.active-keys storm-cluster-state))
        locally-available-active-keys (set/intersection local-list-of-keys all-keys)
        ]
    (log-message "creating list of key entries for blobstore inside zookeeper")
    (doseq [key locally-available-active-keys]
      (.setup-blobstore! storm-cluster-state (:nimbus-host-port-info nimbus) (get-metadata-version blob-store key (get-nimbus-subject)) (str "active"))
      )))

(defn- get-errors [storm-cluster-state storm-id component-id]
  (->> (.errors storm-cluster-state storm-id component-id)
       (map #(doto (ErrorInfo. (:error %) (:time-secs %))
                   (.set_host (:host %))
                   (.set_port (:port %))))))

(defn- get-last-error
  [storm-cluster-state storm-id component-id]
  (if-let [e (.last-error storm-cluster-state storm-id component-id)]
    (doto (ErrorInfo. (:error e) (:time-secs e))
                      (.set_host (:host e))
                      (.set_port (:port e)))))

(defn- thriftify-executor-id [[first-task-id last-task-id]]
  (ExecutorInfo. (int first-task-id) (int last-task-id)))

(def DISALLOWED-TOPOLOGY-NAME-STRS #{"/" "." ":" "\\"})

(defn validate-topology-name! [name]
  (if (some #(.contains name %) DISALLOWED-TOPOLOGY-NAME-STRS)
    (throw (InvalidTopologyException.
            (str "Topology name cannot contain any of the following: " (pr-str DISALLOWED-TOPOLOGY-NAME-STRS))))
  (if (clojure.string/blank? name) 
    (throw (InvalidTopologyException. 
            ("Topology name cannot be blank"))))))

;; We will only file at <Storm dist root>/<Topology ID>/<File>
;; to be accessed via Thrift
;; ex., storm-local/nimbus/stormdist/aa-1-1377104853/stormjar.jar
(defn check-file-access [conf file-path]
  (log-debug "check file access:" file-path)
  (try
    (if (not= (.getCanonicalFile (File. (master-stormdist-root conf)))
          (-> (File. file-path) .getCanonicalFile .getParentFile .getParentFile))
      (throw (AuthorizationException. (str "Invalid file path: " file-path))))
    (catch Exception e
      (throw (AuthorizationException. (str "Invalid file path: " file-path))))))

(defn try-read-storm-conf [conf storm-id]
  (try-cause
    (read-storm-conf-as-nimbus conf storm-id)
    (catch FileNotFoundException e
       (throw (NotAliveException. (str storm-id))))
  )
)

(defn try-read-storm-conf-from-name [conf storm-name nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        id (get-storm-id storm-cluster-state storm-name)]
   (try-read-storm-conf conf id)))

(defn try-read-storm-topology [conf storm-id]
  (try-cause
    (read-storm-topology conf storm-id)
    (catch FileNotFoundException e
       (throw (NotAliveException. (str storm-id))))
  )
)

(defn renew-credentials [nimbus]
  (if (is-leader nimbus :throw-exception false)
    (let [storm-cluster-state (:storm-cluster-state nimbus)
          renewers (:cred-renewers nimbus)
          update-lock (:cred-update-lock nimbus)
          assigned-ids (set (.active-storms storm-cluster-state))]
      (when-not (empty? assigned-ids)
        (doseq [id assigned-ids]
          (locking update-lock
            (let [orig-creds (.credentials storm-cluster-state id nil)
                  topology-conf (try-read-storm-conf (:conf nimbus) id)]
              (if orig-creds
                (let [new-creds (HashMap. orig-creds)]
                  (doseq [renewer renewers]
                    (log-message "Renewing Creds For " id " with " renewer)
                    (.renew renewer new-creds (Collections/unmodifiableMap topology-conf)))
                  (when-not (= orig-creds new-creds)
                    (.set-credentials! storm-cluster-state id new-creds topology-conf)
                    ))))))))
    (log-message "not a leader skipping , credential renweal.")))

(defn validate-topology-size [topo-conf nimbus-conf topology]
  (let [workers-count (get topo-conf TOPOLOGY-WORKERS)
        workers-allowed (get nimbus-conf NIMBUS-SLOTS-PER-TOPOLOGY)
        num-executors (->> (all-components topology) (map-val num-start-executors))
        executors-count (reduce + (vals num-executors))
        executors-allowed (get nimbus-conf NIMBUS-EXECUTORS-PER-TOPOLOGY)]
    (when (and 
           (not (nil? executors-allowed))
           (> executors-count executors-allowed))
      (throw 
       (InvalidTopologyException. 
        (str "Failed to submit topology. Topology requests more than " executors-allowed " executors."))))
    (when (and
           (not (nil? workers-allowed))
           (> workers-count workers-allowed))
      (throw 
       (InvalidTopologyException. 
        (str "Failed to submit topology. Topology requests more than " workers-allowed " workers."))))))

(defserverfn service-handler [conf inimbus]
  (.prepare inimbus conf (master-inimbus-dir conf))
  (log-message "Starting Nimbus with conf " conf)
  (let [nimbus (nimbus-data conf inimbus)
       principal-to-local (AuthUtils/GetPrincipalToLocalPlugin conf)]
    (.prepare ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus) conf)

    ;add to nimbuses
    (.add-nimbus-host! (:storm-cluster-state nimbus) (.toHostPortString (:nimbus-host-port-info nimbus))
      (NimbusSummary.
        (.getHost (:nimbus-host-port-info nimbus))
        (.getPort (:nimbus-host-port-info nimbus))
        (current-time-secs)
        false ;is-leader
        (str (VersionInfo/getVersion))))

    (.addToLeaderLockQueue (:leader-elector nimbus))
    (cleanup-corrupt-topologies! nimbus)
    ;;(setup-code-distributor nimbus)
    (setup-blobstore nimbus)

    ;register call back for code-distributor
    (.code-distributor (:storm-cluster-state nimbus) (fn [] (sync-code conf nimbus)))
    ;; add code to register blobstore
    (when (is-leader nimbus :throw-exception false)
      (doseq [storm-id (.active-storms (:storm-cluster-state nimbus))]
        (transition! nimbus storm-id :startup)))
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-MONITOR-FREQ-SECS)
                        (fn []
                          (when (conf NIMBUS-REASSIGN)
                            (locking (:submit-lock nimbus)
                              (mk-assignments nimbus)))
                          (do-cleanup nimbus)
                          ))
    ;; Schedule Nimbus inbox cleaner
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-CLEANUP-INBOX-FREQ-SECS)
                        (fn []
                          (clean-inbox (inbox nimbus) (conf NIMBUS-INBOX-JAR-EXPIRATION-SECS))
                          ))
    ;;schedule nimbus code sync thread to sync code from other nimbuses.
    (schedule-recurring (:timer nimbus)
      0
      (conf NIMBUS-CODE-SYNC-FREQ-SECS)
      (fn []
        (sync-code conf nimbus)
        ))

    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-CREDENTIAL-RENEW-FREQ-SECS)
                        (fn []
                          (renew-credentials nimbus)))
    (reify Nimbus$Iface
      (^void submitTopologyWithOpts
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
         ^SubmitOptions submitOptions]
        (try
          (is-leader nimbus)
          (assert (not-nil? submitOptions))
          (validate-topology-name! storm-name)
          (check-authorization! nimbus storm-name nil "submitTopology")
          (check-storm-active! nimbus storm-name false)
          (let [topo-conf (from-json serializedConf)]
            (try
              (validate-configs-with-schemas topo-conf)
              (catch IllegalArgumentException ex
                (throw (InvalidTopologyException. (.getMessage ex)))))
            (.validate ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus)
                       storm-name
                       topo-conf
                       topology))
          (swap! (:submitted-count nimbus) inc)
          (let [storm-id (str storm-name "-" @(:submitted-count nimbus) "-" (current-time-secs))
                credentials (.get_creds submitOptions)
                credentials (when credentials (.get_creds credentials))
                topo-conf (from-json serializedConf)
                storm-conf-submitted (normalize-conf
                            conf
                            (-> topo-conf
                              (assoc STORM-ID storm-id)
                              (assoc TOPOLOGY-NAME storm-name))
                            topology)
                req (ReqContext/context)
                principal (.principal req)
                submitter-principal (if principal (.toString principal))
                submitter-user (.toLocal principal-to-local principal)
                topo-acl (distinct (remove nil? (conj (.get storm-conf-submitted TOPOLOGY-USERS) submitter-principal, submitter-user)))
                storm-conf (-> storm-conf-submitted
                               (assoc TOPOLOGY-SUBMITTER-PRINCIPAL (if submitter-principal submitter-principal ""))
                               (assoc TOPOLOGY-SUBMITTER-USER (if submitter-user submitter-user "")) ;Don't let the user set who we launch as
                               (assoc TOPOLOGY-USERS topo-acl)
                               (assoc STORM-ZOOKEEPER-SUPERACL (.get conf STORM-ZOOKEEPER-SUPERACL)))
                storm-conf (if (Utils/isZkAuthenticationConfiguredStormServer conf)
                                storm-conf
                                (dissoc storm-conf STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD))
                total-storm-conf (merge conf storm-conf)
                topology (normalize-topology total-storm-conf topology)
                storm-cluster-state (:storm-cluster-state nimbus)]
            (when credentials (doseq [nimbus-autocred-plugin (:nimbus-autocred-plugins nimbus)]
              (.populateCredentials nimbus-autocred-plugin credentials (Collections/unmodifiableMap storm-conf))))
            (if (and (conf SUPERVISOR-RUN-WORKER-AS-USER) (or (nil? submitter-user) (.isEmpty (.trim submitter-user)))) 
              (throw (AuthorizationException. "Could not determine the user to run this topology as.")))
            (system-topology! total-storm-conf topology) ;; this validates the structure of the topology
            (validate-topology-size topo-conf conf topology)
            (when (and (Utils/isZkAuthenticationConfiguredStormServer conf)
                       (not (Utils/isZkAuthenticationConfiguredTopology storm-conf)))
                (throw (IllegalArgumentException. "The cluster is configured for zookeeper authentication, but no payload was provided.")))
            (log-message "Received topology submission for "
                         storm-name
                         " with conf "
                         (redact-value storm-conf STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD))
            ;; lock protects against multiple topologies being submitted at once and
            ;; cleanup thread killing topology in b/w assignment and starting the topology
            (locking (:submit-lock nimbus)
              (check-storm-active! nimbus storm-name false)
              ;;cred-update-lock is not needed here because creds are being added for the first time.
              (.set-credentials! storm-cluster-state storm-id credentials storm-conf)
              (setup-storm-code nimbus conf storm-id uploadedJarLocation storm-conf topology)
              ;;(.setup-code-distributor! storm-cluster-state storm-id (:nimbus-host-port-info nimbus));; pass the version-info data to create a zk-node too...
              (wait-for-desired-code-replication nimbus total-storm-conf storm-id)
              (.setup-heartbeats! storm-cluster-state storm-id)
              (.setup-backpressure! storm-cluster-state storm-id)
              (let [thrift-status->kw-status {TopologyInitialStatus/INACTIVE :inactive
                                              TopologyInitialStatus/ACTIVE :active}]
                (start-storm nimbus storm-name storm-id (thrift-status->kw-status (.get_initial_status submitOptions))))
              (mk-assignments nimbus)))
          (catch Throwable e
            (log-warn-error e "Topology submission exception. (topology name='" storm-name "')")
            (throw e))))
      
      (^void submitTopology
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology]
        (.submitTopologyWithOpts this storm-name uploadedJarLocation serializedConf topology
                                 (SubmitOptions. TopologyInitialStatus/ACTIVE)))
      
      (^void killTopology [this ^String name]
         (.killTopologyWithOpts this name (KillOptions.)))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (check-storm-active! nimbus storm-name true)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)]
          (check-authorization! nimbus storm-name topology-conf "killTopology"))
        (let [wait-amt (if (.is_set_wait_secs options)
                         (.get_wait_secs options)                         
                         )]
          (transition-name! nimbus storm-name [:kill wait-amt] true)
          ))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
        (check-storm-active! nimbus storm-name true)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)]
          (check-authorization! nimbus storm-name topology-conf "rebalance"))
        (let [wait-amt (if (.is_set_wait_secs options)
                         (.get_wait_secs options))
              num-workers (if (.is_set_num_workers options)
                            (.get_num_workers options))
              executor-overrides (if (.is_set_num_executors options)
                                   (.get_num_executors options)
                                   {})]
          (doseq [[c num-executors] executor-overrides]
            (when (<= num-executors 0)
              (throw (InvalidTopologyException. "Number of executors must be greater than 0"))
              ))
          (transition-name! nimbus storm-name [:rebalance wait-amt num-workers executor-overrides] true)
          ))

      (activate [this storm-name]
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)]
          (check-authorization! nimbus storm-name topology-conf "activate"))
        (transition-name! nimbus storm-name :activate true)
        )

      (deactivate [this storm-name]
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)]
          (check-authorization! nimbus storm-name topology-conf "deactivate"))
        (transition-name! nimbus storm-name :inactivate true))

      (debug [this storm-name component-id enable? samplingPct]
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              storm-id (get-storm-id storm-cluster-state storm-name)
              topology-conf (try-read-storm-conf conf storm-id)
              ;; make sure samplingPct is within bounds.
              spct (Math/max (Math/min samplingPct 100.0) 0.0)
              ;; while disabling we retain the sampling pct.
              debug-options (if enable? {:enable enable? :samplingpct spct} {:enable enable?})
              storm-base-updates (assoc {} :component->debug (if (empty? component-id)
                                                               {storm-id debug-options}
                                                               {component-id debug-options}))]
          (check-authorization! nimbus storm-name topology-conf "debug")
          (when-not storm-id
            (throw (NotAliveException. storm-name)))
          (log-message "Nimbus setting debug to " enable? " for storm-name '" storm-name "' storm-id '" storm-id "' sampling pct '" spct "'"
            (if (not (clojure.string/blank? component-id)) (str " component-id '" component-id "'")))
          (locking (:submit-lock nimbus)
            (.update-storm! storm-cluster-state storm-id storm-base-updates))))

      (uploadNewCredentials [this storm-name credentials]
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              storm-id (get-storm-id storm-cluster-state storm-name)
              topology-conf (try-read-storm-conf conf storm-id)
              creds (when credentials (.get_creds credentials))]
          (check-authorization! nimbus storm-name topology-conf "uploadNewCredentials")
          (locking (:cred-update-lock nimbus) (.set-credentials! storm-cluster-state storm-id creds topology-conf))))

      (beginFileUpload [this]
        (check-authorization! nimbus nil nil "fileUpload")
        (let [fileloc (str (inbox nimbus) "/stormjar-" (uuid) ".jar")]
          (.put (:uploaders nimbus)
                fileloc
                (Channels/newChannel (FileOutputStream. fileloc)))
          (log-message "Uploading file from client to " fileloc)
          fileloc
          ))

      (^void uploadChunk [this ^String location ^ByteBuffer chunk]
        (check-authorization! nimbus nil nil "fileUpload")
        (let [uploaders (:uploaders nimbus)
              ^WritableByteChannel channel (.get uploaders location)]
          (when-not channel
            (throw (RuntimeException.
                    "File for that location does not exist (or timed out)")))
          (.write channel chunk)
          (.put uploaders location channel)
          ))

      (^void finishFileUpload [this ^String location]
        (check-authorization! nimbus nil nil "fileUpload")
        (let [uploaders (:uploaders nimbus)
              ^WritableByteChannel channel (.get uploaders location)]
          (when-not channel
            (throw (RuntimeException.
                    "File for that location does not exist (or timed out)")))
          (.close channel)
          (log-message "Finished uploading file from client: " location)
          (.remove uploaders location)
          ))

      (^String beginFileDownload [this ^String file]
        (check-authorization! nimbus nil nil "fileDownload")
        (check-file-access (:conf nimbus) file)
        (let [is (BufferFileInputStream. file)
              id (uuid)]
          (.put (:downloaders nimbus) id is)
          id
          ))

      (^ByteBuffer downloadChunk [this ^String id]
        (check-authorization! nimbus nil nil "fileDownload")
        (let [downloaders (:downloaders nimbus)
              ^BufferFileInputStream is (.get downloaders id)]
          (when-not is
            (throw (RuntimeException.
                    "Could not find input stream for that id")))
          (let [ret (.read is)]
            (.put downloaders id is)
            (when (empty? ret)
              (.remove downloaders id))
            (ByteBuffer/wrap ret)
            )))

      (^String getNimbusConf [this]
        (check-authorization! nimbus nil nil "getNimbusConf")
        (to-json (:conf nimbus)))

      (^String getTopologyConf [this ^String id]
        (let [topology-conf (try-read-storm-conf conf id)
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getTopologyConf")
              (to-json topology-conf)))

      (^StormTopology getTopology [this ^String id]
        (let [topology-conf (try-read-storm-conf conf id)
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getTopology")
              (system-topology! topology-conf (try-read-storm-topology conf id))))

      (^StormTopology getUserTopology [this ^String id]
        (let [topology-conf (try-read-storm-conf conf id)
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getUserTopology")
              (try-read-storm-topology topology-conf id)))

      (^ClusterSummary getClusterInfo [this]
        (check-authorization! nimbus nil nil "getClusterInfo")
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              supervisor-infos (all-supervisor-info storm-cluster-state)
              ;; TODO: need to get the port info about supervisors...
              ;; in standalone just look at metadata, otherwise just say N/A?
              supervisor-summaries (dofor [[id info] supervisor-infos]
                                          (let [ports (set (:meta info)) ;;TODO: this is only true for standalone

                                            sup-sum (SupervisorSummary. (:hostname info)
                                                                (:uptime-secs info)
                                                                (count ports)
                                                                (count (:used-ports info))
                                                                id) ]
                                            (when-let [version (:version info)] (.set_version sup-sum version))
                                            sup-sum
                                            ))
              bases (topology-bases storm-cluster-state)
              nimbuses (.nimbuses storm-cluster-state)

              ;;update the isLeader field for each nimbus summary
              _ (let [leader (.getLeader (:leader-elector nimbus))
                      leader-host (.getHost leader)
                      leader-port (.getPort leader)]
                  (doseq [nimbus-summary nimbuses]
                    (.set_uptime_secs nimbus-summary (time-delta (.get_uptime_secs nimbus-summary)))
                    (.set_isLeader nimbus-summary (and (= leader-host (.get_host nimbus-summary)) (= leader-port (.get_port nimbus-summary))))))

              topology-summaries (dofor [[id base] bases :when base]
	                                  (let [assignment (.assignment-info storm-cluster-state id nil)
                                                topo-summ (TopologySummary. id
                                                            (:storm-name base)
                                                            (->> (:executor->node+port assignment)
                                                                 keys
                                                                 (mapcat executor-id->tasks)
                                                                 count) 
                                                            (->> (:executor->node+port assignment)
                                                                 keys
                                                                 count)                                                            
                                                            (->> (:executor->node+port assignment)
                                                                 vals
                                                                 set
                                                                 count)
                                                            (time-delta (:launch-time-secs base))
                                                            (extract-status-str base))]
                                               (when-let [owner (:owner base)] (.set_owner topo-summ owner))
                                               (when-let [sched-status (.get @(:id->sched-status nimbus) id)] (.set_sched_status topo-summ sched-status))
                                               (.set_replication_count topo-summ (if (:code-distributor nimbus)
                                                                                   (.getReplicationCount (:code-distributor nimbus) id)
                                                                                   1))
                                               topo-summ
                                          ))]
          (log-message "topology-summ " topology-summaries "\n nimbuses-sum" nimbuses)
          (ClusterSummary. supervisor-summaries
                           topology-summaries
                           nimbuses)
          ))
      
      (^TopologyInfo getTopologyInfoWithOpts [this ^String storm-id ^GetInfoOptions options]
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              topology-conf (try-read-storm-conf conf storm-id)
              storm-name (topology-conf TOPOLOGY-NAME)
              _ (check-authorization! nimbus storm-name topology-conf "getTopologyInfo")
              task->component (storm-task-info (try-read-storm-topology conf storm-id) topology-conf)
              base (.storm-base storm-cluster-state storm-id nil)
              launch-time-secs (if base (:launch-time-secs base) (throw (NotAliveException. (str storm-id))))
              assignment (.assignment-info storm-cluster-state storm-id nil)
              beats (map-val :heartbeat (get @(:heartbeats-cache nimbus) storm-id))
              all-components (-> task->component reverse-map keys)
              num-err-choice (or (.get_num_err_choice options)
                                 NumErrorsChoice/ALL)
              errors-fn (condp = num-err-choice
                          NumErrorsChoice/NONE (fn [& _] ()) ;; empty list only
                          NumErrorsChoice/ONE (comp #(remove nil? %)
                                                    list
                                                    get-last-error)
                          NumErrorsChoice/ALL get-errors
                          ;; Default
                          (do
                            (log-warn "Got invalid NumErrorsChoice '"
                                      num-err-choice
                                      "'")
                            get-errors))
              errors (->> all-components
                          (map (fn [c] [c (errors-fn storm-cluster-state storm-id c)]))
                          (into {}))
              executor-summaries (dofor [[executor [node port]] (:executor->node+port assignment)]
                                        (let [host (-> assignment :node->host (get node))
                                              heartbeat (get beats executor)
                                              stats (:stats heartbeat)
                                              stats (if stats
                                                      (stats/thriftify-executor-stats stats))]
                                          (doto
                                              (ExecutorSummary. (thriftify-executor-id executor)
                                                                (-> executor first task->component)
                                                                host
                                                                port
                                                                (nil-to-zero (:uptime heartbeat)))
                                            (.set_stats stats))
                                          ))
              topo-info  (TopologyInfo. storm-id
                           storm-name
                           (time-delta launch-time-secs)
                           executor-summaries
                           (extract-status-str base)
                           errors
                           )]
            (when-let [owner (:owner base)] (.set_owner topo-info owner))
            (when-let [sched-status (.get @(:id->sched-status nimbus) storm-id)] (.set_sched_status topo-info sched-status))
            (when-let [component->debug (:component->debug base)]
              (.set_component_debug topo-info (map-val converter/thriftify-debugoptions component->debug)))
            (.set_replication_count topo-info (.getReplicationCount (:code-distributor nimbus) storm-id))
            topo-info
          ))

      (^TopologyInfo getTopologyInfo [this ^String storm-id]
        (.getTopologyInfoWithOpts this
                                  storm-id
                                  (doto (GetInfoOptions.) (.set_num_err_choice NumErrorsChoice/ALL))))

      ;;Blobstore implementation code
      (^String beginCreateBlob [this
                                ^String blob-key
                                ^SettableBlobMeta blob-meta]
        (let [session-id (uuid)]
          (.put (:blob-uploaders nimbus)
            session-id
            (->> (ReqContext/context)
              (.subject)
              (.createBlob (:blob-store nimbus) blob-key blob-meta)))
          (log-message "Created blob for " blob-key
            " with session id " session-id)
          (str session-id)))

      (^String beginUpdateBlob [this ^String blob-key]
        (if-let [^AtomicOutputStream os (->> (ReqContext/context)
                                          (.subject)
                                          (.updateBlob (:blob-store nimbus)
                                            blob-key))]
          (let [session-id (uuid)]
            (.put (:blob-uploaders nimbus) session-id os)
            (log-message "Created upload session for " blob-key
              " with id " session-id)
            (str session-id))
          (throw-runtime "Could not find blob for key " blob-key)))

      (^void uploadBlobChunk [this ^String session ^ByteBuffer blob-chunk]
        (let [uploaders (:blob-uploaders nimbus)]
          (if-let [^AtomicOutputStream os (.get uploaders session)]
            (let [chunk-array (.array blob-chunk)
                  remaining (.remaining blob-chunk)
                  array-offset (.arrayOffset blob-chunk)
                  position (.position blob-chunk)]
              (.write os chunk-array (+ array-offset position) remaining)
              (.put uploaders session os))
            (throw-runtime "Blob for session "
              session
              " does not exist (or timed out)"))))

      (^void finishBlobUpload [this ^String session]
        (if-let [^AtomicOutputStream os (.get (:blob-uploaders nimbus) session)]
          (do
            (.close os)
            (log-message "Finished uploading blob for session "
              session
              ". Closing session.")
            (.remove (:blob-uploaders nimbus) session))
          (throw-runtime "Blob for session "
            session
            " does not exist (or timed out)")))

      (^void cancelBlobUpload [this ^String session]
        (if-let [^AtomicOutputStream os (.get (:blob-uploaders nimbus) session)]
          (do
            (.cancel os)
            (log-message "Canceled uploading blob for session "
              session
              ". Closing session.")
            (.remove (:blob-uploaders nimbus) session))
          (throw-runtime "Blob for session "
            session
            " does not exist (or timed out)")))

      (^ReadableBlobMeta getBlobMeta [this ^String blob-key]
        (if-let [^ReadableBlobMeta ret (->> (ReqContext/context)
                                         (.subject)
                                         (.getBlobMeta (:blob-store nimbus)
                                           blob-key))]
          ret
          (throw-runtime "Could not find blob metadata for key " blob-key)))

      (^void setBlobMeta [this ^String blob-key ^SettableBlobMeta blob-meta]
        (->> (ReqContext/context)
          (.subject)
          (.setBlobMeta (:blob-store nimbus) blob-key blob-meta)))

      (^BeginDownloadResult beginBlobDownload [this ^String blob-key]
        (if-let [^InputStreamWithMeta is (->> (ReqContext/context)
                                           (.subject)
                                           (.getBlob (:blob-store nimbus)
                                             blob-key))]
          (let [session-id (uuid)
                ret (BeginDownloadResult. (.getVersion is) (str session-id))]
            (.set_data_size ret (.getFileLength is))
            (.put (:blob-downloaders nimbus) session-id (BufferInputStream. is ^Integer (Utils/getInt (conf STORM-BLOBSTORE-INPUTSTREAM-BUFFER-SIZE-BYTES) (int 65536))))
            (log-message "Created download session for " blob-key
              " with id " session-id)
            ret)
          (throw-runtime "Could not find blob for key " blob-key)))

      (^ByteBuffer downloadBlobChunk [this ^String session]
        (let [downloaders (:blob-downloaders nimbus)
              ^BufferInputStream is (.get downloaders session)]
          (when-not is
            (throw (RuntimeException.
                     "Could not find input stream for session " session)))
          (let [ret (.read is)]
            (.put downloaders session is)
            (when (empty? ret)
              (.close is)
              (.remove downloaders session))
            (log-debug "Sending " (alength ret) " bytes")
            (ByteBuffer/wrap ret))))

      (^void deleteBlob [this ^String blob-key]
        (let [subject (->> (ReqContext/context)
                           (.subject))]
        (.deleteBlob (:blob-store nimbus) blob-key subject)
        (.setup-blobstore! blob-key (:nimbus-host-port-info nimbus) (get-metadata-version (:blob-store nimbus) blob-key subject) (str "deleted"))
        (log-message "Deleted blob for key " blob-key)))

      (^ListBlobsResult listBlobs [this ^String session]
        (let [listers (:blob-listers nimbus)
              ^Iterator keys-it (if (clojure.string/blank? session)
                                  (->> (ReqContext/context)
                                    (.subject)
                                    (.listKeys (:blob-store nimbus)))
                                  (.get listers session))
              _ (or keys-it (throw-runtime "Blob list for session "
                              session
                              " does not exist (or timed out)"))

              ;; Create a new session id if the user gave an empty session string.
              ;; This is the use case when the user wishes to list blobs
              ;; starting from the beginning.
              session (if (clojure.string/blank? session)
                        (let [new-session (uuid)]
                          (log-message "Creating new session for downloading list " new-session)
                          new-session)
                        session)]
          (if-not (.hasNext keys-it)
            (do
              (.remove listers session)
              (log-message "No more blobs to list for session " session)
              ;; A blank result communicates that there are no more blobs.
              (ListBlobsResult. (java.util.ArrayList. 0) (str session)))
            (let [^List list-chunk (->> keys-it
                                     (iterator-seq)
                                     (take 100) ;; Limit to next 100 keys
                                     (java.util.ArrayList.))
                  _ (log-message session " downloading " (.size list-chunk) " entries")]
              (.put listers session keys-it)
              (ListBlobsResult. list-chunk (str session))))))

      (^BlobReplication getBlobReplication [this ^String blob-key]
        (->> (ReqContext/context)
          (.subject)
          (.getBlobReplication (:blob-store nimbus) blob-key)))

      (^BlobReplication updateBlobReplication [this ^String blob-key ^int replication]
        (->> (ReqContext/context)
          (.subject)
          (.updateBlobReplication (:blob-store nimbus) blob-key replication)))
      ;;Blobstore implementation code ends

      Shutdownable
      (shutdown [this]
        (log-message "Shutting down master")
        (cancel-timer (:timer nimbus))
        (.disconnect (:storm-cluster-state nimbus))
        (.cleanup (:downloaders nimbus))
        (.cleanup (:uploaders nimbus))
        (.close (:leader-elector nimbus))
        (if (:code-distributor nimbus) (.close (:code-distributor nimbus) (:conf nimbus)))
        (log-message "Shut down master")
        )
      DaemonCommon
      (waiting? [this]
        (timer-waiting? (:timer nimbus))))))

(defmethod mk-code-distributor :distributed [conf]
  (let [code-distributor (new-instance (conf STORM-CODE-DISTRIBUTOR-CLASS))]
    (.prepare code-distributor conf)
    code-distributor))

(defmethod mk-code-distributor :local [conf]
  nil)

(defn download-code [conf nimbus storm-id host port]
  (let [tmp-root (str (master-tmp-dir conf) file-path-separator (uuid))
        storm-cluster-state (:storm-cluster-state nimbus)
        storm-root (master-stormdist-root conf storm-id)
        remote-meta-file-path (master-storm-metafile-path storm-root)
        local-meta-file-path (master-storm-metafile-path tmp-root)]
    (FileUtils/forceMkdir (File. tmp-root))
    (Utils/downloadFromHost conf remote-meta-file-path local-meta-file-path host port)
    (if (:code-distributor nimbus)
      (.download (:code-distributor nimbus) storm-id (File. local-meta-file-path)))
    (if (.exists (File. storm-root)) (FileUtils/forceDelete (File. storm-root)))
    (FileUtils/moveDirectory (File. tmp-root) (File. storm-root))
    (.setup-code-distributor! storm-cluster-state storm-id (:nimbus-host-port-info nimbus))))

(defmethod sync-code :distributed [conf nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        code-ids (set (code-ids (:conf nimbus)))
        active-topologies (set (.code-distributor storm-cluster-state (fn [] (sync-code conf nimbus))))
        missing-topologies (set/difference active-topologies code-ids)]
    (if (not (empty? missing-topologies))
      (do
        (.removeFromLeaderLockQueue (:leader-elector nimbus))
        (doseq [missing missing-topologies]
          (log-message "missing topology " missing " has state on zookeeper but doesn't have a local dir on this host.")
          (let [nimbuses-with-missing (.code-distributor-info storm-cluster-state missing)]
            (log-message "trying to download missing topology code from " (clojure.string/join "," nimbuses-with-missing))
            (doseq [nimbus-host-port nimbuses-with-missing]
              (when-not (contains? (code-ids (:conf nimbus)) missing)
                (try
                  (download-code conf nimbus missing (.getHost nimbus-host-port) (.getPort nimbus-host-port))
                  (catch Exception e (log-error e "Exception while trying to sync-code for missing topology" missing)))))))))

    (if (empty? (set/difference active-topologies (set (code-ids (:conf nimbus)))))
      (.addToLeaderLockQueue (:leader-elector nimbus)))))

(defmethod sync-code :local [conf nimbus]
  nil)

(defn launch-server! [conf nimbus]
  (validate-distributed-mode! conf)
  (let [service-handler (service-handler conf nimbus)
        server (ThriftServer. conf (Nimbus$Processor. service-handler) 
                              ThriftConnectionType/NIMBUS)]
    (add-shutdown-hook-with-force-kill-in-1-sec (fn []
                                                  (.shutdown service-handler)
                                                  (.stop server)))
    (log-message "Starting Nimbus server...")
    (.serve server)
    service-handler))

;; distributed implementation

(defmethod setup-jar :distributed [conf tmp-jar-location stormroot]
           (let [src-file (File. tmp-jar-location)]
             (if-not (.exists src-file)
               (throw
                (IllegalArgumentException.
                 (str tmp-jar-location " to copy to " stormroot " does not exist!"))))
             (FileUtils/copyFile src-file (File. (master-stormjar-path stormroot)))
             ))

;; local implementation

(defmethod setup-jar :local [conf & args]
  nil
  )

(defn -launch [nimbus]
  (let [conf (merge
               (read-storm-config)
               (read-yaml-config "storm-cluster-auth.yaml" false))]
  (launch-server! conf nimbus)))

(defn standalone-nimbus []
  (reify INimbus
    (prepare [this conf local-dir]
      )
    (allSlotsAvailableForScheduling [this supervisors topologies topologies-missing-assignments]
      (->> supervisors
           (mapcat (fn [^SupervisorDetails s]
                     (for [p (.getMeta s)]
                       (WorkerSlot. (.getId s) p))))
           set ))
    (assignSlots [this topology slots]
      )
    (getForcedScheduler [this]
      nil )
    (getHostName [this supervisors node-id]
      (if-let [^SupervisorDetails supervisor (get supervisors node-id)]
        (.getHost supervisor)))
    ))

(defn -main []
  (setup-default-uncaught-exception-handler)
  (-launch (standalone-nimbus)))
