(ns my-app.core
  (:gen-class)
  (:require [jackdaw.streams :as js]
            [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]
            [jackdaw.serdes.edn :refer [serde]]
            [clojure.spec.alpha :as s]))

;; The config for our Kafka Streams app
(def kafka-config
  {"application.id" "my-app"
   "bootstrap.servers" "localhost:9092"
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})

;; Serdes tell Kafka how to serialize/deserialize messages Don't know much about this....
(def serdes
  {:key-serde (serde)
   :value-serde (serde)})

;; Each topic needs a config. The important part to note is the :topic-name key.
(def first-topic
  (merge {:topic-name "first"
          :partition-count 1
          :replication-factor 1
          :topic-config {}}
         serdes))


(def second-topic
  (merge {:topic-name "second"
          :partition-count 1
          :replication-factor 1
          :topic-config {}}
         serdes))

;; An admin client is needed to do things like create and delete topics
(def admin-client (ja/->AdminClient kafka-config))


(defn to-produce! [somenumber]
  "Publish a message to the  first-topic, with the specified number"
  (let [user-id     (rand-int 10000)
        quantity    (inc (rand-int 10))]
    (with-open [producer (jc/producer kafka-config serdes)]
      @(jc/produce! producer first-topic user-id {:user-id user-id
                                                              :data somenumber
                                                              :quantity quantity}))))

(defn to-view [topic]
  "View the messages on the given topic"
  (with-open [consumer (jc/subscribed-consumer (assoc kafka-config "group.id" (str (java.util.UUID/randomUUID)))
                                               [topic])]
    (jc/seek-to-beginning-eager consumer)
    (->> (jcl/log-until-inactivity consumer 100)
         (map :value)
         doall)))


(defn topology [builder]
      ;; Read the first-topic into a KStream
  (-> (js/kstream builder first-topic)
      ;; Filter the KStream for data greater than 100
      (js/filter (fn [[_ data]]
                   (<= 100 (:data data))))
      (js/map (fn [[key data]]
                [key (select-keys data [:data :user-id])]))
      ;; Write our KStream to the large-transaction-made topic
      (js/to second-topic)))


(defn start! []
  "Starts the topology"
  (let [builder (js/streams-builder)]
    (topology builder)
    (doto (js/kafka-streams builder kafka-config)
      (js/start))))

(defn stop! [kafka-streams-app]
  "Stops the given KafkaStreams application"
  (js/close kafka-streams-app))
