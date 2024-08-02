from typing import List, Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    kafka_broker_address: Optional[str] = None
    cities: List[str]
    kafka_topic: str

 

    @field_validator('kafka_topic')
    @classmethod
    def validate_kafka_topic(cls, value):
        assert value in {
            'weather_daily_forecast',
            'weather_weekly_forecast',
            'weather_hourly_forecast',
        }, f'Invalid value for kafka_topic: {value}'
        return value


config = Config()