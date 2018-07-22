(ns kafka-utils.core
  (:require [clojure.core.async :as async]
            [datascript.core :as d]
            [franzy.common.configuration.codec :as franzy.config]
            [clojure.core.async.impl.protocols :refer [closed?]])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord)
           (java.util UUID)))


(defn ->chan
  ([consumer] (->chan consumer 500))
  ([^KafkaConsumer consumer poll-timeout]
   (let [out-chan (async/chan)]
     (try
       (async/go-loop [rs (.poll consumer poll-timeout)]
         (doseq [^ConsumerRecord r rs]
           (let [record-value (.value r)
                 value-map (if (string? record-value)
                             {:kafka.record/value record-value}
                             record-value)
                 clj-record (merge {:kafka.record/key       (.key r)
                                    :kafka.record/timestamp (.timestamp r)
                                    :kafka.record/offset    (.offset r)
                                    :kafka.record/topic     (.topic r)}
                                   value-map)]
             (async/>! out-chan clj-record)))
         (if-not (closed? out-chan)
           (recur (.poll consumer poll-timeout))
           (.close consumer)))
       (catch Exception e
         (async/close! out-chan)
         (throw e)))
     out-chan)))


(defn read-from-beginning [topic-name bootstrap-servers key-deserializer value-deserializer]
  (let [consumer-config (franzy.config/encode {:bootstrap.servers bootstrap-servers
                                               :group.id          (str (UUID/randomUUID))})
        consumer (KafkaConsumer. consumer-config key-deserializer value-deserializer)]
    (.subscribe consumer [topic-name])
    ;; need to poll for consumer to be assigned partitions
    (.poll consumer 0)
    (.seekToBeginning consumer (.assignment consumer))
    (->chan consumer)))


(defn read-from-end [topic-name bootstrap-servers key-deserializer value-deserializer]
  (let [consumer-config (franzy.config/encode {:bootstrap.servers bootstrap-servers
                                               :group.id          (str (UUID/randomUUID))})
        consumer (KafkaConsumer. consumer-config key-deserializer value-deserializer)]
    (.subscribe consumer [topic-name])
    (->chan consumer)))


(defn sink-to-db!
  [chan db-conn]
  (async/go-loop [record (async/<! chan)]
    (d/transact! db-conn [(->> (assoc record :db/id (Math/abs (hash (str (:kafka.record/offset record)
                                                                         "::"
                                                                         (:kafka.record/topic record)))))
                               (remove (fn [[k v]] (nil? v)))
                               (into {}))])
    (when-let [next (async/<! chan)]
      (recur next))))


(defn latest-only [db id-attr]
  (let [latest-updates (->> db
                            (d/q `[:find ?e ?offset ?id
                                   :where [?e :kafka.record/offset ?offset]
                                   [?e ~id-attr ?id]])
                            (group-by (fn [[_ _ record-id]] record-id))
                            (map (fn [[_ xs]]
                                   (->> xs
                                        (sort-by (fn [[_ offset _]] offset))
                                        (map (fn [[entity-id _ _]] entity-id))
                                        last)))
                            set)]
    (d/filter db (fn [_ datom]
                   (contains? latest-updates (:e datom))))))


(defn latest-update-before [db timestamp id id-attr updated-at-attr]
  (->> (d/q `[:find ?e ?t
              :in $ ?before
              :where
              [$ ?e ~updated-at-attr ?t]
              [$ ?e ~id-attr ~id]
              [(< ?t ?before)]]
            db timestamp)
       (sort-by second)
       last
       first
       (d/entity db)))


(defn apply-transducer [xf in-chan]
  (let [out-chan (async/chan)]
    (async/pipeline 1 out-chan xf in-chan)
    out-chan))