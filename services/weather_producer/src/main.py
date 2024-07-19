from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from typing import List
import json

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from time import sleep

from loguru import logger

def weather_update(
    kafka_broker_address:str,
    kafka_topic: str,
    cities: List[str],
    
    
                   
                   
                   )->None:
    
    """
    Reads weather data from Weather API and writes it to Kafka.
    
    Args:
        kafka_broker_address (str): Address of the Kafka broker.
        kafka_topic (str): Name of the topic in Kafka.
        cities (List[str]): List of city names.
        
    Returns:
        None
    """
    producer = None
    while not producer:
        try:
            producer = Producer({"bootstrap.servers": kafka_broker_address})
        except Exception as e:
            print(f"brokers not ready - {e}")
            sleep(0.1)
    admin = AdminClient({"bootstrap.servers": kafka_broker_address})

    # Create input topic
    input_topic = NewTopic(kafka_topic, num_partitions=1, replication_factor=1)
    admin.create_topics([input_topic])
    logger.info(f"input topic {kafka_topic} created successfully")

    event = {'id':1,'text':"Hello World"} 
   
    # Add data to input topic
    
    while True:
        producer.produce(
            kafka_topic,
            key = json.dumps(event['id']).encode('utf-8'),
            value = json.dumps(event).encode('utf-8')
        )
        
        sleep(.5)
        





if __name__ == '__main__':

    # import os
    # breakpoint()

    try:
        weather_update(
            kafka_broker_address='localhost:19092',
            kafka_topic='weather',
            cities=['San Diego'],
        )
    except KeyboardInterrupt:
        logger.info('Exiting...')