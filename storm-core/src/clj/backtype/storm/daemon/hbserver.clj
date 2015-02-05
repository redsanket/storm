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
(ns backtype.storm.daemon.hbserver
  (:import [java.nio ByteBuffer]
   [backtype.storm.generated HBServer]
   [java.util.concurrent ConcurrentHashMap])
  (:import [java.io FileNotFoundException])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:import [backtype.storm.security.auth ThriftServer ThriftConnectionType ReqContext AuthUtils])
  (:import [backtype.storm.generated HBServer HBServer$Processor
            HBServer$Iface AuthorizationException HBExecutionException HBRecords Pulse HBNodes])
  (:import [backtype.storm.daemon Shutdownable])
  (:use [clojure.string :only [replace-first split]])
  (:use [backtype.storm bootstrap util log])
  (:use [backtype.storm.config :only [validate-configs-with-schemas read-storm-config]])
  (:use [backtype.storm.daemon common])
  (:gen-class))

(defn hb-data [conf]
  (ConcurrentHashMap.))

(defserverfn service-handler
  [conf]
  (log-message "Starting HBServer with conf " conf)
  (let [heartbeats ^ConcurrentHashMap (hb-data conf)]
    (reify HBServer$Iface
      (^void createPath [this ^String path])

      (^boolean exists [this ^String path]
       (let [it-does (.exists heartbeats path)
             _ (log-debug (str "Checking if path [" path "] exists..." it-does "."))]
         it-does))

      (^void sendPulse [this ^Pulse pulse]
       (let [id (.get_id pulse)
             details (.get_details pulse)
             _ (log-debug (str "Saving Pulse for id [" id "] data [" + (str details) "]."))]
         (.put heartbeats id details)))

      (^HBRecords getAllPulseForPath [this ^String idPrefix])

      (^HBNodes getAllNodesForPath [this ^String idPrefix]
       (let [_ (log-debug "List all nodes for path " idPrefix)]
         (HBNodes. (distinct (for [k (.keySet heartbeats)
                         :let [trimmed-k (second (split (replace-first k idPrefix "") #"/"))]
                         :when (= (.indexOf k idPrefix) 0)]
                     trimmed-k)))))

      (^Pulse getPulse [this ^String id]
       (let [details (.get heartbeats id)
             _ (log-debug (str "Getting Pulse for id [" id "]...data " (str details) "]."))]
         (doto (Pulse. ) (.set_id id) (.set_details details))))

      (^void deletePath [this ^String idPrefix]
       (let [prefix (if (= \/ (last idPrefix)) idPrefix (str idPrefix "/"))]
         (doseq [k (.keySet heartbeats)
               :when (= (.indexOf k prefix) 0)]
         (.deletePulseId this k))))

      (^void deletePulseId [this ^String id]
       (let [_  (log-message (str "Deleting Pulse for id [" id "]."))]
         (.remove heartbeats id)))

      Shutdownable
      (shutdown [this]
        (log-message "Shutting down hbserver"))

    DaemonCommon
    (waiting? [this]
      ())
    )))

(defn launch-server! []
  (let [conf (read-storm-config)
        service-handler (service-handler conf)
        server (ThriftServer. conf (HBServer$Processor. service-handler)
                                   ThriftConnectionType/HBSERVER)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.shutdown service-handler) (.stop server))))
    (log-message "Starting hbserver...")
    (.serve server)
    service-handler))

(defn -main []
  (launch-server!))
