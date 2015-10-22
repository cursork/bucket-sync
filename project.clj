(defproject bucket-sync "0.1.0-SNAPSHOT"
  :license {:name "MIT"
            :url  "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure     "1.7.0"]
                 [org.clojure/core.async  "0.1.346.0-17112a-alpha"]
                 [org.clojure/data.codec  "0.1.0"]
                 [amazonica               "0.3.35"]
                 [com.taoensso/timbre     "4.1.1"]
                 [environ                 "1.0.0"]]
  :main bucket-sync.core)
