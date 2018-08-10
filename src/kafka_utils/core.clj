(ns kafka-utils.core
  (:require [clojure.core.async :as async]
            [datascript.core :as d]
            [franzy.common.configuration.codec :as franzy.config]
            [clojure.core.async.impl.protocols :refer [closed?]])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord)
           (java.util UUID)))


(defn ->chan
  "Reads messages from a Kafka topic, using the provided consumer, into a core.async channel.

  Each message in the topic will be put on the channel individually.

  Each message is augmented with these keys:
    - :kafka.record/key
    - :kafka.record/timestamp
    - :kafka.record/offset
    - :kafka.record/topic
    - :kafka.record/partition"
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
                                    :kafka.record/topic     (.topic r)
                                    :kafka.record/partition (.partition r)}
                                   value-map)]
             (async/>! out-chan clj-record)))
         (if-not (closed? out-chan)
           (recur (.poll consumer poll-timeout))
           (.close consumer)))
       (catch Exception e
         (async/close! out-chan)
         (throw e)))
     out-chan)))


(defn read-from-beginning
  "Reads a topic from the beginning (i.e. offset 0) of a topic into a core.async channel"
  [topic-name bootstrap-servers key-deserializer value-deserializer]
  (let [consumer-config (franzy.config/encode {:bootstrap.servers bootstrap-servers
                                               :group.id          (str (UUID/randomUUID))})
        consumer (KafkaConsumer. consumer-config key-deserializer value-deserializer)]
    (.subscribe consumer [topic-name])
    ;; need to poll for consumer to be assigned partitions
    (.poll consumer 0)
    (.seekToBeginning consumer (.assignment consumer))
    (->chan consumer)))


(defn read-from-end
  "Reads a topic from the end (i.e. the latest offset) of a topic into a core.async channel."
  [topic-name bootstrap-servers key-deserializer value-deserializer]
  (let [consumer-config (franzy.config/encode {:bootstrap.servers bootstrap-servers
                                               :group.id          (str (UUID/randomUUID))})
        consumer (KafkaConsumer. consumer-config key-deserializer value-deserializer)]
    (.subscribe consumer [topic-name])
    (->chan consumer)))


(defn sink-to-db!
  "Takes each message on a core.async channel, and inserts it into the provided datascript db.

  The datascript id of each message is calculated from the :kafka.record/offset, :kafka.record/partition and :kafka.record/topic keys."
  [chan db-conn]
  (async/go-loop [record (async/<! chan)]
    (d/transact! db-conn [(->> (assoc record :db/id (Math/abs (hash (str (:kafka.record/offset record)
                                                                         "::"
                                                                         (:kafka.record/partition record)
                                                                         "::"
                                                                         (:kafka.record/topic record)))))
                               (remove (fn [[k v]] (nil? v)))
                               (into {}))])
    (when-let [next (async/<! chan)]
      (recur next))))


(defn latest-only
  "Returns a db containing only the latest message for each entity. This is useful for changelog topics.

  Entities are determined using the 'id-attr' parameter.

  The offset is used to determine which message is the latest one."
  [db id-attr]
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


(defn as-of
  "Returns the state of the db at the given time, as determined by the Kafka record timestamp"
  [db timestamp]
  (let [valid-entities (->> (d/q '[:find ?e
                                   :in $ ?before
                                   :where [$ ?e :kafka.record/timestamp ?t]
                                   [(< ?t ?before)]]
                                 db timestamp)
                            set)]
    (d/filter db (fn [_ datom]
                   (contains? valid-entities (:e datom))))))


(defn apply-transducer
  "Takes a channel, applies a transducer to it, and returns the new channel. The original channel is not modified."
  [xf in-chan]
  (let [out-chan (async/chan)]
    (async/pipeline 1 out-chan xf in-chan)
    out-chan))
