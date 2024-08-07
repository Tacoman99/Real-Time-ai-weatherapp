import json
import requests
import os
from loguru import logger

from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSourceMessage, KafkaSinkMessage
from bytewax.connectors.kafka import operators as kop
from time import sleep
from src.llm import weather_llm 

def build_flow(
    kafka_broker: list, 
    kafka_topic: list,
    output_topic: str,
    ) -> Dataflow:
    """
    Creates a bytewax dataflow updating the forecast with the weather description using an LLM 
    
    Args:
        kafka_broker (list): Kafka broker urls
        kafka_topic (list): Kafka topics to read from
        output_topic (str): Kafka topic to write to
    
    Returns:
        Dataflow: The configured Bytewax dataflow
    """
    flow = Dataflow("redpanda-enrichment")

    # Kafka input configuration
    kinp = kop.input("redpanda-in", flow, brokers=kafka_broker, topics=kafka_topic)

    # Unpack the Kafka input operator result
    input_stream = kinp.oks

    # Add inspection step
    inspected = op.inspect(
        
        "inspect-redpanda-oks",
        input_stream,
        lambda step_id, msg: logger.info(f"Received message in step {step_id}: key={msg.key}, value={msg.value.decode('ascii')}")
    )

    # Here you would add more processing steps...
    
    weather_description = weather_llm(personality = 'snarky', model_name = 'llama3.1:latest')
    
    enriched_stream = op.map("enrich", kinp.oks, weather_description.AI_Forecast )
    op.inspect("inspect-enriched", enriched_stream)
    kop.output("kafka-out", enriched_stream, brokers=kafka_broker, topic = output_topic)


    return flow

# This is the flow that Bytewax will look for
flow = build_flow(kafka_broker=['localhost:19092'], kafka_topic=['weather_daily_forecast'],output_topic='AI_forecast')

if __name__ == "__main__":
    try:
        # The flow will be run by Bytewax, so we don't need to call flow.run() here
        flow = build_flow(
            kafka_broker=['localhost:19092'], 
            kafka_topic=['weather_daily_forecast'],
            output_topic='AI_forcast')

    except KeyboardInterrupt:
        logger.info('Exiting Neatly .... ')
