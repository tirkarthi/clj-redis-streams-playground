(ns producer
  (:require [taoensso.carmine :as car :refer (wcar)]
            [clj-time.core :as t]
            [clj-time.coerce :as c]))

;; Redis init

(def server1-conn {:pool {} :spec {:host "127.0.0.1" :port 6379}})
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

(defn gen-epoch
  "Generate epoch timeseries for a given date"
  [year month day]
  (for [hour   (range 0 24)
        minute (range 0 60)
        sec    (range 0 60)]
    (quot (c/to-long (t/date-time year month day hour minute sec)) 1000)))

(defn -main
  [& args]
  (let [timestamps   (gen-epoch 2018 8 15)
        temperatures (repeatedly 86400 #(rand-nth (range 20 30)))
        start        (c/to-long (t/now))]
    (doseq [[timestamp temperature] (map vector timestamps temperatures)]
      (wcar* (car/xadd :chennai timestamp :sensor 1 :temperature temperature)))
    (println "Inserted records successfully in "
             (quot (- (c/to-long (t/now)) start) 1000)
             " seconds")))
