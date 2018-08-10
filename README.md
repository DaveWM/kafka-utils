# kafka-utils
[![Clojars Project](https://img.shields.io/clojars/v/kafka-utils.svg)](https://clojars.org/kafka-utils)

The aim of this library is to make it easier to query data in Kafka topics.

[Full Documentation](https://davewm.github.io/kafka-utils/)

## Why?

When working with Kafka, you often want to query the data in a topic (or multiple topics).
There are a couple of common ways of doing this:

* Read all the messages on a topic using the console consumer, filter them using grep (or some other shell utility). 
This is relatively straightforward to do, and doesn't require any setup. 
However, using this method it's difficult to perform complex queries and to join data from multiple topics.  
* Sink the topic to a database using Kafka Connect, and run a query in there. 
This allows you to run arbitrary queries across multiple topics. However, there are some drawbacks:
  * You have to set up, and maintain, a Kafka Connect job for every topic you want to query.
  * You have to set up and maintain the DB.
  * If you want to query messages on a topic not being sinked to the DB, you have to set up a Kafka Connect job first. This is time consuming, and can be very painful if you run lots of ad-hoc queries.
  * When you sink changelog topics to an SQL DB, the database only holds the latest state of each entity - previous states are overridden. This makes querying the history of an entity impossible.  

This library aims to address the issues with the approaches above. The overall approach is:
1. In the repl, you read Kafka topics into core.async channels.
2. Optionally, you can apply a transducer to these channels to filter/transform the data.
3. Sink the channels into an in-memory DataScript DB.
4. Use Datalog to query the DB.
 
This has several advantages:
* You get a very powerful query language (Datalog) to query the topic data. 
* You can easily do joins across topics.
* There is no setup required, so ad-hoc queries become much easier.
* There is nothing to maintain.
* You can query the history of a changelog topic.

However, there are some caveats:

* The amount of data you can query is limited by the amount of memory you have. If you need to query millions of records, one of the other approaches outlined above may be more suitable.

## How?

The easiest way to use this library is:

1. Call `read-from-beginning` to get a core.async channel of all the messages in a topic.
2. Create a datascript DB by calling `datascript.core/create-conn`.
2. Call `sink-to-db!` to sink the channel to the DB.
4. Wait for the messages to be loaded into the DB (should take less than a minute)

See [the docs](https://davewm.github.io/kafka-utils/) for more info.

## Usage

1. Add `[kafka.utils "0.1.1"]` as a dependency.
2. Start up your repl, and enter these commands:

```clojure
;; require kafka-utils and datascript
(require '[kafka-utils.core :as ku]
         '[datascript.core :as d])

;; add the name of the topic you want to query here
(def topic-name ...)

 ;; Change this if you're not running Kafka locally 
(def broker-url "localhost:9092")

;; this should be a Kafka Deserializer http://bit.ly/2vWUFQd
(def deserializer ...)

;; this should be a Kafka Serializer http://bit.ly/2vVPhwG
(def serializer ...)

;; Get a core.async channel of all the records in a topic.
;; For the serdes, I'd recommend using franzy, but this isn't required
(def channel
    (ku/read-from-beginning topic-name [broker-url]
                            serializer
                            deserializer))

;; Create a datascript db
(def conn (d/create-conn))

;; Sink the topic data into the datascript db
(ku/sink-to-db! channel db-conn)
```

3. Once the data has all been loaded into the db, you can now query it.
Each entity will have one attribute per record field, and also these fields:
`:kafka.record/key`, `:kafka.record/timestamp`, `:kafka.record/offset`, and `:kafka.record/topic`.
Here are some example queries:

```clojure
;; returns a list of topics, with a count of messages
(d/q '[:find ?topic (count ?m)
       :where [?m :kafka.record/topic ?topic]]
       @conn)
       
;; given a changelog topic, returns the latest state of each entity
(d/q '[:find [(pull [*] ?e) ...]
       :where [?e :kafka.record/topic "topic-name"]]
       (ku/latest-only @conn :id) ;; effectively compacts the messages by :id
       )
```

## License

Distributed under the GPL V3.
