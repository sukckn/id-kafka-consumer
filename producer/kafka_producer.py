''' List all output parameters as comma-separated values in the "Output:" docString. Do not specify "None" if there is no output parameter.'''
''' List all Python packages that are not built-in packages in the "DependentPackages:" docString. Separate the package names with commas on a single line. '''
''' DependentPackages: confluent-kafka'''
from confluent_kafka import Producer
import json
import logging

logger= logging.getLogger("SAS_Kafka_Producer_Logger")
err_msg= ''

##################################################################################################
kafka_server= "kafka-service.kafka.svc.cluster.local:9092"

conf= {
    "bootstrap.servers": kafka_server,
    "acks": "all",
    "linger.ms": 10,
    "message.timeout.ms": 5000
    }

##################################################################################################
def delivery_report(err, msg):
    global err_msg
    if err:
        err_msg= f'Delivery failed: {err}'
        logger.error(err_msg)
    else:
        logger.debug(f'Delivered to {msg.topic()} [{msg.partition()}] @{msg.offset()}')

##################################################################################################
def execute (decision,payload,session_id):
    'Output:err_msg'
    global err_msg

    logger.info('Kafka Producer')

    if not session_id:
        session_id= None

    producer= Producer(conf)
    producer.produce(
        topic=decision,
        key=session_id,
        value=payload,
        on_delivery=delivery_report,
    )
    producer.poll(0.1)

    producer.flush(10.0)

    return err_msg
