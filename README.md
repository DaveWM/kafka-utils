# kafka-utils

Utilities to make querying the data in kafka topics easier.

## Usage

1. Add `[kafka.utils "0.0.1"]` as a dependency.
2. Start up your repl, and call ``
3. Start up your repl, and enter these commands:

```clojure
;; require kafka-utils and datascript
(require '[kafka-utils.core :as ku]
         '[datascript.core :as d])

;; Get a core.async channel of all the records in a topic.
;; For the serdes, I'd recommend using franzy, but this isn't required
(def channel
    (ku/read-from-beginning "backblaze_smart" ["localhost:9092"]
                            (franzy.deserializers/string-deserializer)
                            (franzy.json.deserializers/json-deserializer {:key-fn true})))

;; Create a datascript db
(def conn (d/create-conn))

;; Sink the topic data into the datascript db
(ku/sink-to-db! channel db-conn)
```

4. Once the data has all been loaded into the db, you can now query it.
Each entity will have one attr per record field, and also these fields:
`kafka.record/key`, `kafka.record/timestamp`, `kafka.record/offset`, and `kafka.record/topic`.

## License

Distributed under the GPL V3.
