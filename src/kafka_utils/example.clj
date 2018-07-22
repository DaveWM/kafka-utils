(ns kafka-utils.example
  (:require [franzy.serialization.json.deserializers :as franzy.json.deserializers]
            [franzy.serialization.deserializers :as franzy.deserializers]
            [franzy.common.configuration.codec :as franzy.config]
            [kafka-utils.core :as kafka-utils]
            [datascript.core :as d])
  (:import (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.clients.consumer KafkaConsumer)))

(comment
  (def consumer (KafkaConsumer. (franzy.config/encode {:bootstrap.servers ["localhost:9092"],
                                                       :group.id          (str (java.util.UUID/randomUUID))})
                                (franzy.deserializers/string-deserializer)
                                (franzy.json.deserializers/json-deserializer {:key-fn true})))
  (.subscribe consumer ["backblaze_smart"])
  (.poll consumer 100)
  (.seekToBeginning consumer [(TopicPartition. "backblaze_smart" 0)])
  (def c (kafka-utils/->chan consumer))
  (def conn (d/create-conn))
  (kafka-utils/sink-to-db! c conn))