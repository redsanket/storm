(ns backtype.storm.timer
  (:import [backtype.storm.utils Time])
  (:import [java.util PriorityQueue Comparator])
  (:import [java.util.concurrent Semaphore])
  (:use [backtype.storm util log])
  )

;; The timer defined in this file is very similar to java.util.Timer, except it integrates with
;; Storm's time simulation capabilities. This lets us test code that does asynchronous work on the timer thread

(defnk mk-timer [:kill-fn (fn [& _] ) :thread-name nil]
  (let [queue (PriorityQueue. 10
                              (reify Comparator
                                (compare [this o1 o2]
                                  (- (first o1) (first o2))
                                  )
                                (equals [this obj]
                                  true
                                  )))
        active (atom true)
        lock (Object.)
        notifier (Semaphore. 0)
        thread-fn (fn []
                    (while @active
                      (try
                        (let [[time-millis _ _ :as elem] (locking lock (.peek queue))]
                          (if (and elem (>= (current-time-millis) time-millis))
                            ;; imperative to not run the function inside the timer lock
                            ;; otherwise, it's possible to deadlock if function deals with other locks
                            ;; (like the submit lock)
                            (let [afn (locking lock (second (.poll queue)))]
                              (afn))
                            (if time-millis ;; if any events are scheduled
                              ;; sleep until event generation
                              ;; note that if any recurring events are scheduled then we will always go through
                              ;; this branch, sleeping only the exact necessary amount of time
                              (Time/sleep (- time-millis (current-time-millis)))
                              ;; else poll to see if any new event was scheduled
                              ;; this is in essence the response time for detecting any new event schedulings when
                              ;; there are no scheduled events
                              (Time/sleep 1000))
                            ))
                        (catch Throwable t
                          ;; because the interrupted exception can be wrapped in a runtimeexception
                          (when-not (exception-cause? InterruptedException t)
                            (kill-fn t)
                            (reset! active false)
                            (throw t))
                          )))
                    (.release notifier))
        timer-thread (if (clojure.string/blank? thread-name)
                       (Thread. thread-fn)
                       (Thread. thread-fn thread-name))]
    (.setDaemon timer-thread true)
    (.setPriority timer-thread Thread/MAX_PRIORITY)
    (.start timer-thread)
    {:timer-thread timer-thread
     :queue queue
     :active active
     :lock lock
     :cancel-notifier notifier}))

(defn- check-active! [timer]
  (when-not @(:active timer)
    (throw (IllegalStateException. "Timer is not active"))))

(defnk schedule [timer delay-secs afn :check-active true]
  (when check-active (check-active! timer))
  (let [id (uuid)
        ^PriorityQueue queue (:queue timer)]
    (locking (:lock timer)
      (.add queue [(+ (current-time-millis) (* 1000 (long delay-secs))) afn id])
      )))

(defn schedule-recurring [timer delay-secs recur-secs afn]
  (schedule timer
            delay-secs
            (fn this []
              (afn)
              (schedule timer recur-secs this :check-active false)) ; this avoids a race condition with cancel-timer
            ))

(defn cancel-timer [timer]
  (check-active! timer)
  (locking (:lock timer)
    (reset! (:active timer) false)
    (.interrupt (:timer-thread timer)))
  (.acquire (:cancel-notifier timer)))

(defn timer-waiting? [timer]
  (Time/isThreadWaiting (:timer-thread timer)))
