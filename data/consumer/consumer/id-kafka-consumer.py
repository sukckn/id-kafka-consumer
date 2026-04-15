from confluent_kafka import Consumer, KafkaError
from datetime import datetime
import json
import logging
import sys
import os
import json
import requests
import signal

#########################################################################################
logger= logging.getLogger("SAS_Kafka_Consumer_Logger")
logging.basicConfig(level=logging.ERROR,format="%(asctime)s %(levelname)s %(message)s")

consumer= None
kafka_server_default= "kafka-service.kafka.svc.cluster.local:9092"
kafka_server= None
kafka_topics= []
consumer_group= "SAS_SCR"
subscribers= {}
conf= {}
#########################################################################################
def setConsumerConfig():
    global conf

    # Read configuration from environment variables with defaults for poll interval and records
    poll_interval= int(os.getenv("POLL_INTERVAL", 300000))

    conf= {
        "bootstrap.servers": kafka_server,
        "group.id": consumer_group,            
        "security.protocol": "PLAINTEXT",    
        "auto.offset.reset": "earliest",         # start from beginning if no commit
        "enable.auto.offset.store": False,       # manual offset store (recommended for at-least-once processing)
        "enable.auto.commit": False,             # explicit commit (recommended)
        "max.poll.interval.ms": poll_interval,   # Prevent rebalances due to slow processing
        }

#########################################################################################
def shutdown(signum, frame):
    logger.info("Shutting down consumer...")
    consumer.close()
    sys.exit(0)

#########################################################################################
def getSubscribers():
    subscribers_env= {
        key: value
        for key, value in os.environ.items()
        if key.startswith("CONSUMER_SCR_")
    }
    subscribers= {}
    for key, value in subscribers_env.items():
        subscribers= subscribers | {value.split(":", 1)[0]:value.split(":", 1)[1]}    
        
    return subscribers

#########################################################################################
def callSCR(scrInput, topic):
    rc= True

    url= subscribers[topic] #get url for the subscribed topics 
    payload= json.dumps(scrInput)  
    headers= {
        'Content-Type': 'application/json'
        }
    
    logger.debug("Calling SCR on %s", url)
    response= requests.request("POST", url, headers=headers, data=payload, verify=False)

    if response.status_code != 200:
        logger.error("Failed to call SCR for topic %s. HTTP Status: %d, Response: %s", topic, response.status_code, response.text)
        rc= False
        return rc

    logger.debug("HTTP Status: %d", response.status_code)
    logger.debug("HTTP Response: %s", response.text)
    return rc

#########################################################################################
def run_consumer():
    global consumer
    consumer = Consumer(conf)

    logger.info("Subscribing to topics: %s", kafka_topics)
    consumer.subscribe(kafka_topics)

    try:
        while True:
            msg= consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        "End of partition %s [%d] at offset %d",
                        msg.topic(), msg.partition(), msg.offset()
                    )
                else:
                    logger.error("Kafka error: %s", msg.error())
                continue

            # MESSAGE RECEIVED
            try:
                key= msg.key().decode("utf-8") if msg.key() else None
                value= json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                logger.exception(f"Failed to decode message: {str(e)}")
                continue

            logger.debug("Received message")
            logger.debug("  Topic: %s", msg.topic())
            logger.debug("  Partition: %s", msg.partition())
            logger.debug("  Offset: %s", msg.offset())
            logger.debug("  Key: %s", key)
            logger.debug("  Value: %s", json.dumps(value, indent=2))
            
            rc= callSCR(value, msg.topic())                    
            # Commit offset only AFTER successful processing
            if rc:
                consumer.store_offsets(msg)
                consumer.commit(msg)
            else:
                logger.error("Failed to process message, not committing offset for topic %s, partition %d, offset %d", msg.topic(), msg.partition(), msg.offset())

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")

    finally:
        logger.info("Closing Consumer")
        consumer.close()

#########################################################################################
if __name__ == "__main__":
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} INFO Started SAS ID Kafka Consumer...")

    #set the log level accoring to the env var LOG_LEVEL. If not set default to ERROR
    log_level= os.getenv("LOG_LEVEL", "ERROR").upper()
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} INFO Log level set to: {log_level}")
    logger.setLevel(level=getattr(logging, log_level, logging.ERROR))
    run= True

    kafka_server= os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not kafka_server:
        logger.warning(f"Kafka server not set. Using default url: {kafka_server_default}")
        kafka_server= kafka_server_default

    if run:
        subscribers= getSubscribers()
        if not subscribers:
            logger.error("No subcribers set! Ensure environment variables 'CONSUMEER_SCR_...' are set.")
            run= False

    if run:
        kafka_topics= list(subscribers.keys())
        if not kafka_topics:
            logger.error("No Kafka topics found! Ensure environment variables 'CONSUMEER_SCR_...' are set correctly.")
            run= False
    if run:
        setConsumerConfig() 
        run_consumer()