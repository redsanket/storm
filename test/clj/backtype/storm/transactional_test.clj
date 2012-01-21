(ns backtype.storm.transactional-test
  (:use [clojure test])
  (:import [backtype.storm.topology TopologyBuilder])
  (:import [backtype.storm.transactional TransactionalSpoutCoordinator ITransactionalSpout
            ITransactionalSpout$Coordinator])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])  
  )

(bootstrap)

;; Testing TODO:
;; 
;; * Test that commit isn't considered successful until the entire tree has been completed (including tuples emitted from commit method)
;;      - test the full topology (this is a test of acking/anchoring)
;; * Test that batch isn't considered processed until the entire tuple tree has been completed
;;      - test the full topology (this is a test of acking/anchoring)
;; * Test that it picks up where it left off when restarting the topology
;;      - run topology and restart it
;; * Test that coordinator and partitioned state are cleaned up properly (and not too early) - test rotatingtransactionalstate
;; * Test that it repeats the meta for a partitioned state (test partitioned emitter on its own)
;; * Test that partitioned state emits nothing for the partition if it has seen a future transaction for that partition (test partitioned emitter on its own)

(defn mk-coordinator-state-changer [atom]
  (TransactionalSpoutCoordinator.
    (reify ITransactionalSpout
      (getComponentConfiguration [this]
        nil)
      (getCoordinator [this conf context]
        (reify ITransactionalSpout$Coordinator
          (initializeTransaction [this txid prevMetadata]
            @atom )
          (close [this]
            )))
    )))

(def BATCH-STREAM TransactionalSpoutCoordinator/TRANSACTION_BATCH_STREAM_ID)
(def COMMIT-STREAM TransactionalSpoutCoordinator/TRANSACTION_COMMIT_STREAM_ID)

(defn mk-spout-capture [capturer]
  (SpoutOutputCollector.
    (reify ISpoutOutputCollector
      (emit [this stream-id tuple message-id]
        (swap! capturer update-in [stream-id]
          (fn [oldval] (concat oldval [{:tuple tuple :id message-id}])))
        []
        ))))

(defn verify-and-reset! [expected-map emitted-map-atom]
  (let [results @emitted-map-atom]
    (dorun
     (map-val
      (fn [tuples]
        (doseq [t tuples]
          (is (= (-> t :tuple first) (:id t)))
          ))
      results))
    (is (= expected-map
           (map-val
            (fn [tuples]
              (map (comp #(update % 0 (memfn getTransactionId))
                         vec
                         :tuple)
                   tuples))
            results
            )))
    (reset! emitted-map-atom {})
    ))

(defn get-attempts [capture-atom stream]
  (map :id (get @capture-atom stream)))

(defn get-commit [capture-atom]
  (-> @capture-atom (get COMMIT-STREAM) first :id))

(deftest test-coordinator
  (let [zk-port (available-port 2181)
        coordinator-state (atom nil)
        emit-capture (atom nil)]
    (with-inprocess-zookeeper zk-port
      (letlocals
        (bind coordinator
              (mk-coordinator-state-changer coordinator-state))
        (.open coordinator
               (merge (read-default-config)
                       {TOPOLOGY-MAX-SPOUT-PENDING 4
                       TOPOLOGY-TRANSACTIONAL-ID "abc"
                       STORM-ZOOKEEPER-PORT 2181
                       STORM-ZOOKEEPER-SERVERS ["localhost"]
                       })
               nil
               (mk-spout-capture emit-capture))
        (reset! coordinator-state 10)
        (.nextTuple coordinator)
        (bind attempts (get-attempts emit-capture BATCH-STREAM))
        (verify-and-reset! {BATCH-STREAM [[1 10] [2 10] [3 10] [4 10]]}
                           emit-capture)

        (.nextTuple coordinator)
        (verify-and-reset! {} emit-capture)
        
        (.fail coordinator (second attempts))
        (bind new-second-attempt (first (get-attempts emit-capture BATCH-STREAM)))
        (verify-and-reset! {BATCH-STREAM [[2 10]]} emit-capture)
        (is (not= new-second-attempt (second attempts)))
        (.ack coordinator new-second-attempt)
        (verify-and-reset! {} emit-capture)
        (.ack coordinator (first attempts))
        (bind commit-id (get-commit emit-capture))
        (verify-and-reset! {COMMIT-STREAM [[1]]} emit-capture)

        (reset! coordinator-state 12)
        (.ack coordinator commit-id)
        (bind commit-id (get-commit emit-capture))
        (verify-and-reset! {COMMIT-STREAM [[2]] BATCH-STREAM [[5 12]]} emit-capture)
        (.ack coordinator commit-id)
        (verify-and-reset! {BATCH-STREAM [[6 12]]} emit-capture)

        (.fail coordinator (nth attempts 2))
        (bind new-third-attempt (first (get-attempts emit-capture BATCH-STREAM)))
        (verify-and-reset! {BATCH-STREAM [[3 10]]} emit-capture)

        (.ack coordinator new-third-attempt)
        (bind commit-id (get-commit emit-capture))
        (verify-and-reset! {COMMIT-STREAM [[3]]} emit-capture)

        (.ack coordinator (nth attempts 3))
        (verify-and-reset! {} emit-capture)

        (.fail coordinator commit-id)
        (bind new-third-attempt (first (get-attempts emit-capture BATCH-STREAM)))
        (verify-and-reset! {BATCH-STREAM [[3 10]]} emit-capture)

        (.ack coordinator new-third-attempt)
        (bind commit-id (get-commit emit-capture))
        (verify-and-reset! {COMMIT-STREAM [[3]]} emit-capture)

        (.nextTuple coordinator)
        (verify-and-reset! {} emit-capture)
        
        (.ack coordinator commit-id)
        (verify-and-reset! {COMMIT-STREAM [[4]] BATCH-STREAM [[7 12]]} emit-capture)
        ))))


(defn mk-bolt-capture [capturer]
  (BoltOutputCollector.
    (reify IOutputCollector
      (emit [this stream-id anchors values]
        (swap! capturer update-in [stream-id]
               (fn [oldval] (concat oldval [values])))
        []
        )
      (ack [this tuple]
        )
      (fail [this tuple]
        )
      )))

(deftest no-partial-commit
  ;; * Test that transactionalbolts only commit when they've received the whole batch for that attempt,
  ;;   not a partial batch - test on its own.
  ;;   test that it fails before finishBatch has been called, (with no tuples or with tuples for the attempt), commits otherwise
  
  ;;  what about testing that the coordination is done properly?
  ;;  can check that it receives all the prior tuples before finishbatch is called in the full topology
  ;;  should test that it commits even when receiving no tuples (and test that finishBatch is called before commit in this case)
  )
