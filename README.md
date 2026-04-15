# id-kafka
To call SCR containers asyncrouniously from another decision flow you can use Kafka.
The calling decision flow can put the payload for the SCr to be called in a Kafka topic and a consumer will read the Kafka topic and calls the appropriate SCR container with the necessary payload.

The consumer is in a docker container and can get configured via some environment variables.

For a real time system where you have a lot of TPS (e.g. 500) you should partition the Kafka topics and start one consumer per topic (set replicas in yaml file).
The number of required partitions/consumers depends on the TPS and the latency of the SCR to be called. Monitor the messages in the topic and check if the Consumer Lag is constantly growing, as this may be critical. In this case you want to adjust the partitions and also consumers.
