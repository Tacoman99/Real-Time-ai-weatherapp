from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from typing import List
import json

from src.config import config

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from time import sleep

from loguru import logger
from src.open_weather_api import weather_api

def kafka_messages(
    kafka_topic:str, 
    producer: object, 
    data: dict, 
    key: str, 
    admin: object,
    ) -> None:
    
    """
    Generates a kafka topic using confluent admin client then sends data to the  topic using 
    confluent producer
    
    Args: 
        kafka_topic (str): name of the topic to be generated.
        producer (object): confluent producer
        data (dict): dictionary containing the message to be sent to the topic.
        key (str): key of the message.
        admin (object): confluent admin
    Returns:
        None
    """
    input_topic = NewTopic(kafka_topic, num_partitions=1, replication_factor=1)
    admin.create_topics([input_topic])
    logger.info(f"input topic {kafka_topic} created successfully")
    
    producer.produce(
                kafka_topic,
                key = json.dumps(data[key]).encode('utf-8'),
                value = json.dumps(data).encode('utf-8')
            )



    
        

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
    #TODO 
        # - seperate weekly, daily, hourly data to different topics. DONE
        # - weekly forecast should be updated twice a day
        
    producer = None
    while not producer:
        try:
            producer = Producer({"bootstrap.servers": kafka_broker_address})
        except Exception as e:
            print(f"brokers not ready - {e}")
            sleep(0.1)
    admin = AdminClient({"bootstrap.servers": kafka_broker_address})
    
    # need to return this coordinates some how using api or a internet tool 
    latitude = 32.7157
    longitude = -117.1647

    weekly_weather = weather_api(city=cities, latitude=latitude, longitude=longitude)

    if kafka_topic == "weather_hourly_forecast":
        
        forecast_data = weekly_weather.hourly_forecast()
        # Add data to input topic
        
        kafka_messages(
            kafka_topic = kafka_topic, 
            producer = producer, 
            data = forecast_data, 
            key = 'date',
            admin=admin,
            )
        
        producer.flush()
        logger.info(f"Successfully updated forecast to {kafka_topic} topic")

    elif kafka_topic == "weather_daily_forecast":
        
        # keeps returning current weather data everying 30 seconds. probably change to a hr? look into
        while True:
            forecast_data = weekly_weather.daily_forecast()
            
            # Add data to input topic
            kafka_messages(
                kafka_topic = kafka_topic, 
                producer = producer, 
                data = forecast_data, 
                key = 'timestamp',
                admin=admin,
                )
            
            producer.flush()
            logger.info(f"Successfully updated forecast to {kafka_topic} topic")
            sleep(30)

    else:
        
        kafka_topic = "weather_weekly_forecast"
        forecast_data = weekly_weather.weekly_forecast()
        
        # Add data to input topic
        kafka_messages(
            kafka_topic = kafka_topic, 
            producer = producer, 
            data = forecast_data, 
            key = 'date',
            admin=admin,
            )
  
        producer.flush()
        
        logger.info(f"Successfully updated forecast to {kafka_topic} topic")
        
            





if __name__ == '__main__':

    # import os
    # breakpoint()
    
    
    
    logger.debug('Configuration:')
    logger.debug(config.model_dump())

    try:
        weather_update(
            kafka_broker_address=config.kafka_broker_address,
            kafka_topic=config.kafka_topic,
            cities=config.cities,
        )
    except KeyboardInterrupt:
        logger.info('Exiting...')