from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import PromptTemplate
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage

from loguru import logger


def describe_weather(
  kafka_topic:str,
  personality:str,
  model_name:str,
  model_template:str,
  
  
    
    
    )->None:

    """
    Consumes weather data from a kafka topic and uses a llm to describe the forcast in mutiple different
    personalities i.e sarcastic, humorous etc.
    
    Args:
        kafka_topic (str): The name of the kafka topic to consume from.
        personality (str): The name of the personality to use when generating the response.
        model_name (str): The name of the llm model to use for generation.
        model_template (str): The template to use when generating the response.
    Returns:
        None
    """
    model_template = """ You are tasked to provide a detail weather report of a city for a fake city called\
        Wakanada. The weather report should be in the form of a sentence. You can use any words you want but\
        have a snarky personality when describing the weather.
    """
    
    model_prompt = PromptTemplate(
        template=model_template)
    model = ChatOllama(model='llama3.1:latest')
    
    llm_chain = model_prompt | model
    
    weather_report = llm_chain.invoke({'cities': 'Wakanada'})
    
    logger.debug(f'Weather report: {weather_report}')
if __name__ == '__main__':

    
    # logger.debug('Configuration:')
    # logger.debug(config.model_dump())

    try:
        describe_weather(
            kafka_topic='weather_daily_forecast',
            personality='taco',
            model_name='llama3-7b-hf',
            model_template = 'place',
        )
    except KeyboardInterrupt:
        logger.info('Exiting...')
        
        