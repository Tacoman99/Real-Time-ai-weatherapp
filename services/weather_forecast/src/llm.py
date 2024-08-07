from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import PromptTemplate
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage,KafkaSourceMessage
from bytewax.connectors.kafka import operators as kop

import json 
from loguru import logger


class weather_llm:
    
    def __init__(
        self,
        personality: str,
        model_name: str,
    ):
        self.personality = personality
        self.model_name = model_name
        
    
    def AI_Forecast(
        self,
        msg:KafkaSourceMessage)-> str:
        
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
        
        current_weather = json.loads( msg.value.decode("ascii") )
        logger.info(f"Processing weather: {current_weather}")

        model_template = """ You are tasked to provide a detail weather report of a city for a fake city called\
            Wakanada. The weather report should be in the form of a sentence. You can use any words you want but\
            have a snarky personality when describing the weather. Here is the current weather report: {current_weather}
        """
        
        model_prompt = PromptTemplate( 
                                      template= model_template,
                                      input_variables=["current_weather", 'cities'],)
        model = ChatOllama(model='llama3.1:latest')
        
        llm_chain = model_prompt | model
        
        # will replace with the actual city name
        weather_report = llm_chain.invoke({'cities': 'Wakanada',"current_weather":current_weather})

                
        logger.debug(f'Weather report: {weather_report}')
        
        current_weather['forecast'] = weather_report.content

    
        return  KafkaSinkMessage(key=msg.key, value=json.dumps(current_weather).encode())

        