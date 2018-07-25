(defproject kafka-utils "0.1.1"
  :description "utils for querying kafka topics"
  :url "https://github.com/DaveWM/kafka-utils"
  :license {:name "GPL V3"
            :url "https://www.gnu.org/licenses/gpl-3.0.en.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/kafka_2.12 "1.1.1" :scope "provided"]
                 [org.clojure/core.async "0.4.474"]
                 [datascript "0.16.6"]
                 [ymilky/franzy "0.0.1"]
                 [ymilky/franzy-json "0.0.1"]])
