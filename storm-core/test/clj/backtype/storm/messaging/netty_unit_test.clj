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
(ns backtype.storm.messaging.netty-unit-test
  (:use [clojure test])
  (:import [backtype.storm.messaging TransportFactory])
  (:use [backtype.storm testing util config log])
  (:use [backtype.storm.daemon.worker :only [is-connection-ready]])
  (:import [java.util ArrayList]))

(def port 6700)
(def task 1)

;; In a "real" cluster (or an integration test), Storm itself would ensure that a topology's workers would only be
;; activated once all the workers' connections are ready.  The tests in this file however launch Netty servers and
;; clients directly, and thus we must ensure manually that the server and the client connections are ready before we
;; commence testing.  If we don't do this, then we will lose the first messages being sent between the client and the
;; server, which will fail the tests.
(defn- wait-until-ready
  ([connections]
      (do (log-message "Waiting until all Netty connections are ready...")
          (wait-until-ready connections 0)))
  ([connections waited-ms]
    (let [interval-ms 10
          max-wait-ms 5000]
      (if-not (every? is-connection-ready connections)
        (if (<= waited-ms max-wait-ms)
          (do
            (Thread/sleep interval-ms)
            (wait-until-ready connections (+ waited-ms interval-ms)))
          (throw (RuntimeException. (str "Netty connections were not ready within " max-wait-ms " ms"))))
        (log-message "All Netty connections are ready")))))

(deftest test-basic
  (log-message "Should send and receive a basic message")
  (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    TOPOLOGY-KRYO-FACTORY "backtype.storm.serialization.DefaultKryoFactory"
                    TOPOLOGY-TUPLE-SERIALIZER "backtype.storm.serialization.types.ListDelegateSerializer"
                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false
                    }
        context (TransportFactory/makeContext storm-conf)
        port (available-port 6700)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)
        _ (wait-until-ready [server client])
        _ (.send client task (.getBytes req_msg))
        iter (.recv server 0 0)
        resp (.next iter)]
    (is (= task (.task resp)))
    (is (= req_msg (String. (.message resp))))
    (.close client)
    (.close server)
    (.term context)))

;;TODO need to debug load is not comming back, and it is hanging.
;;(deftest test-load
;;  (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
;;        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
;;                    STORM-MESSAGING-NETTY-AUTHENTICATION false
;;                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
;;                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
;;                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
;;                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
;;                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
;;                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
;;                    TOPOLOGY-KRYO-FACTORY "backtype.storm.serialization.DefaultKryoFactory"
;;                    TOPOLOGY-TUPLE-SERIALIZER "backtype.storm.serialization.types.ListDelegateSerializer"
;;                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
;;                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false
;;                    }
;;        context (TransportFactory/makeContext storm-conf)
;;        port (available-port 6700)
;;        server (.bind context nil port)
;;        client (.connect context nil "localhost" port)
;;        _ (wait-until-ready [server client])
;;        _ (.send client task (.getBytes req_msg))
;;        iter (.recv server 0 0)
;;        resp (.next iter)
;;        _ (.sendLoadMetrics server {(int 1) 0.0 (int 2) 1.0})
;;        _ (while-timeout 5000 (empty? (.getLoad client [(int 1) (int 2)])) (Thread/sleep 10))
;;        load (.getLoad client [(int 1) (int 2)])]
;;    (is (= 0.0 (.getBoltLoad (.get load (int 1)))))
;;    (is (= 1.0 (.getBoltLoad (.get load (int 2)))))
;;    (is (= task (.task resp)))
;;    (is (= req_msg (String. (.message resp))))
;;    (.close client)
;;    (.close server)
;;    (.term context)))

(deftest test-large-msg
  (log-message "Should send and receive a large message")
  (let [req_msg (apply str (repeat 2048000 'c'))
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 102400
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    TOPOLOGY-KRYO-FACTORY "backtype.storm.serialization.DefaultKryoFactory"
                    TOPOLOGY-TUPLE-SERIALIZER "backtype.storm.serialization.types.ListDelegateSerializer" 
                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false
                    }
        context (TransportFactory/makeContext storm-conf)
        port (available-port 6700)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)
        _ (wait-until-ready [server client])
        _ (.send client task (.getBytes req_msg))
        iter (.recv server 0 0)
        resp (.next iter)]
    (is (= task (.task resp)))
    (is (= req_msg (String. (.message resp))))
    (.close client)
    (.close server)
    (.term context)))    
    
;;TODO test needs to be updated connection now throws away messages when it is not connected
;; so we need to check status in different situations.
;;(deftest test-server-delayed
;;    (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
;;       storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
;;                    STORM-MESSAGING-NETTY-AUTHENTICATION false
;;                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
;;                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
;;                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000 
;;                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
;;                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
;;                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
;;                    TOPOLOGY-KRYO-FACTORY "backtype.storm.serialization.DefaultKryoFactory"
;;                    TOPOLOGY-TUPLE-SERIALIZER "backtype.storm.serialization.types.ListDelegateSerializer" 
;;                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
;;                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false
;;                    }
;;        context (TransportFactory/makeContext storm-conf)
;;        port (available-port 6700)
;;        client (.connect context nil "localhost" port)
;;        
;;        server (Thread.
;;                (fn []
;;                  (Thread/sleep 1000)
;;                  (let [server (.bind context nil port)
;;                        iter (.recv server 0 0)
;;                        resp (.next iter)]
;;                    (is (= task (.task resp)))
;;                    (is (= req_msg (String. (.message resp))))
;;                    (.close server) 
;;                  )))
;;        _ (.start server)
;;        _ (.send client task (.getBytes req_msg))
;;        ]
;;    (.join server)
;;    (.close client)
;;    (.term context)))


(deftest test-batch
  (let [num-messages 100000
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024000
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    TOPOLOGY-KRYO-FACTORY "backtype.storm.serialization.DefaultKryoFactory"
                    TOPOLOGY-TUPLE-SERIALIZER "backtype.storm.serialization.types.ListDelegateSerializer"
                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false
                    }
        _ (log-message "Should send and receive many messages (testing with " num-messages " messages)")
        context (TransportFactory/makeContext storm-conf)
        port (available-port 6700)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)
        _ (wait-until-ready [server client])]
    (doseq [num  (range 1 num-messages)]
      (let [req_msg (str num)]
        (.send client task (.getBytes req_msg))))

    (let [resp (ArrayList.)
          received (atom 0)]
      (while (< @received (- num-messages 1))
        (let [iter (.recv server 0 0)]
          (while (.hasNext iter)
            (let [msg (.next iter)]
              (.add resp msg)
              (swap! received inc)
              ))))
      (doseq [num  (range 1 num-messages)]
      (let [req_msg (str num)
            resp_msg (String. (.message (.get resp (- num 1))))]
        (is (= req_msg resp_msg)))))

    (.close client)
    (.close server)
    (.term context)))

;;TODO test needs to be updated this is not really true any more
;; we might just want to delete this test.
;;(deftest test-server-always-reconnects
;;    (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
;;       storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
;;                    STORM-MESSAGING-NETTY-AUTHENTICATION false
;;                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
;;                    STORM-MESSAGING-NETTY-MAX-RETRIES 2
;;                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 10 
;;                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 50
;;                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
;;                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
;;                    TOPOLOGY-KRYO-FACTORY "backtype.storm.serialization.DefaultKryoFactory"
;;                    TOPOLOGY-TUPLE-SERIALIZER "backtype.storm.serialization.types.ListDelegateSerializer" 
;;                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
;;                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false
;;                    }
;;        context (TransportFactory/makeContext storm-conf)
;;        port (available-port 6700)
;;        client (.connect context nil "localhost" port)
;;        _ (.send client task (.getBytes req_msg))
;;        _ (Thread/sleep 1000)
;;        server (.bind context nil port)
;;        iter (future (.recv server 0 0))
;;        resp (deref iter 5000 nil)
;;        resp-val (if resp (.next resp) nil)]
;;    (is resp-val)
;;    (when resp-val
;;      (is (= task (.task resp-val)))
;;      (is (= req_msg (String. (.message resp-val)))))
;;    (.close client)
;;    (.close server)
;;    (.term context)))
