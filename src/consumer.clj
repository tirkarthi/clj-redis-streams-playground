(ns consumer
  (:require [taoensso.carmine :as car :refer (wcar)]
            [clj-time.core :as t]
            [clj-time.coerce :as c]))

;; Redis init

(def server1-conn {:pool {} :spec {:host "127.0.0.1" :port 6379}})
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

(defn process-message
  [message]
  (println "Processing message : " message)
  (Thread/sleep 1000))

(defn -main
  [& args]
  (let [consumer (keyword (nth args 0))]
    (println "Processing messages by consumer : " (name consumer))
    (loop []
      (let [message (wcar* (car/xreadgroup :block 0 :group :mygroup consumer
                                           :count 1 :streams :chennai ">"))]
        (process-message message))
      (recur))))
