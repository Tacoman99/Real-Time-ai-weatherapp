import openmeteo_requests

import requests_cache
import pandas as pd
from datetime import datetime 
from retry_requests import retry
from loguru import logger

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
class weather_api: 
	
	def __init__(
		self,
		city: str,
		latitude: int,
		longitude: int,
		params: dict = None, 
		
	):
		self.latitude = latitude,
		self.longitude = longitude,
		self.current_date = datetime.today()
		self.today_formatted_date = self.current_date.strftime("%Y-%m-%d")
  
	def daily_forecast(
     self,
     params: dict = None,
     ):
		params = {
						"latitude": self.latitude,
						"longitude": self.longitude,
						"current": ["relative_humidity_2m", "apparent_temperature", "is_day", "precipitation", "cloud_cover", "pressure_msl", "wind_speed_10m", "wind_gusts_10m"],
						"temperature_unit": "fahrenheit",
						"wind_speed_unit": "ms",
						"precipitation_unit": "inch",
						"timeformat": "unixtime",
						
					}
		response = openmeteo.weather_api(url, params=params)[0]

		# Current values. The order of variables needs to be the same as requested.
		current = response.Current()
		current_weather = {
			'timestamp':current.Time(),
			"current_relative_humidity_2m": current.Variables(0).Value(),
			"current_apparent_temperature": current.Variables(1).Value(),
			"current_is_day": current.Variables(2).Value(),
			"current_precipitation": current.Variables(3).Value(),
			"current_cloud_cover": current.Variables(4).Value(),
			"current_pressure_msl": current.Variables(5).Value(),
		}
		logger.info(f'Current weather: {current_weather}')
		return current_weather
	
	def hourly_forecast(
     self,
     params: dict = None,
     ):
		
		params = {
						"latitude": self.latitude,
						"longitude": self.longitude,
						"current": ["apparent_temperature", "is_day", "precipitation", "weather_code", "cloud_cover", "wind_speed_10m"],
						"hourly": ["temperature_2m", "apparent_temperature", "precipitation_probability", "surface_pressure", "cloud_cover"],
						"daily": ["temperature_2m_max", "temperature_2m_min", "sunrise", "sunset", "sunshine_duration", "uv_index_max", "uv_index_clear_sky_max", "precipitation_probability_max", "wind_speed_10m_max"],
						"temperature_unit": "fahrenheit",
						"wind_speed_unit": "ms",
						"precipitation_unit": "inch",
						"timeformat": "unixtime",
						"start_date": self.today_formatted_date,
						"end_date": self.today_formatted_date,
					}
		response = openmeteo.weather_api(url, params=params)[0]
     
		# Process hourly data. The order of variables needs to be the same as requested.
		hourly =response.Hourly()
		hourly_apparent_temperature = hourly.Variables(0).ValuesAsNumpy()
		hourly_cloud_cover = hourly.Variables(1).ValuesAsNumpy()
		hourly_visibility = hourly.Variables(2).ValuesAsNumpy()

		# Retrieving the current time and rounding down to the nearest hour

  
		hourly_data = {"date": pd.date_range(
			start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
			end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
			freq = pd.Timedelta(seconds = hourly.Interval()),
			inclusive = "left"
		)}
		hourly_data["apparent_temperature"] = hourly_apparent_temperature
		hourly_data["cloud_cover"] = hourly_cloud_cover
		hourly_data["visibility"] = hourly_visibility

		hourly_dataframe = pd.DataFrame(data = hourly_data)
		
		# Get the current time
		current_time = datetime.now()

		# Round down to the nearest hour
		rounded_time = current_time.replace(minute=0, second=0, microsecond=0)
		# Convert to pandas Timestamp with UTC timezone
		timestamp = pd.Timestamp(rounded_time, tz='UTC')
		
		# Get the data from the dataframe that is greater than or equal to the rounded timestamp
		present_data = hourly_dataframe[hourly_dataframe['date'] >= timestamp]
  
		present_data['date'] = present_data['date'].astype('int64') // 10**9

  
		return present_data.to_dict(orient="list")
	
  
  
	def weekly_forecast(
     self,
     params: dict = None):
		params = {
						"latitude": self.latitude,
						"longitude": self.longitude,
						"current": ["apparent_temperature", "is_day", "precipitation", "weather_code", "cloud_cover", "wind_speed_10m"],
						"hourly": ["temperature_2m", "apparent_temperature", "precipitation_probability", "surface_pressure", "cloud_cover"],
						"daily": ["temperature_2m_max", "temperature_2m_min", "sunrise", "sunset", "sunshine_duration", "uv_index_max", "uv_index_clear_sky_max", "precipitation_probability_max", "wind_speed_10m_max"],
						"temperature_unit": "fahrenheit",
						"wind_speed_unit": "ms",
						"precipitation_unit": "inch",
						"timeformat": "unixtime",
					}
		response = openmeteo.weather_api(url, params=params)[0]
		
		
		daily = response.Daily()
		daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
		daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
		daily_sunrise = daily.Variables(2).ValuesAsNumpy()
		daily_sunset = daily.Variables(3).ValuesAsNumpy()
		daily_sunshine_duration = daily.Variables(4).ValuesAsNumpy()
		daily_uv_index_max = daily.Variables(5).ValuesAsNumpy()
		daily_uv_index_clear_sky_max = daily.Variables(6).ValuesAsNumpy()
		daily_precipitation_probability_max = daily.Variables(7).ValuesAsNumpy()
		daily_wind_speed_10m_max = daily.Variables(8).ValuesAsNumpy()
  
		daily_data = {"date": pd.date_range(
				start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
				end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
				freq = pd.Timedelta(seconds = daily.Interval()),
				inclusive = "left"
			)}
  
		daily_data['date'] = daily_data['date'].astype('int64') // 10**9
		daily_data["temperature_2m_max"] = daily_temperature_2m_max
		daily_data["temperature_2m_min"] = daily_temperature_2m_min
		daily_data["sunrise"] = daily_sunrise
		daily_data["sunset"] = daily_sunset
		daily_data["sunshine_duration"] = daily_sunshine_duration
		daily_data["uv_index_max"] = daily_uv_index_max
		daily_data["uv_index_clear_sky_max"] = daily_uv_index_clear_sky_max
		daily_data["precipitation_probability_max"] = daily_precipitation_probability_max
		daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max

		daily_dataframe = pd.DataFrame(data = daily_data)
		logger.info(f"\n{daily_dataframe}")
	
		return daily_dataframe.to_dict(orient="list")
