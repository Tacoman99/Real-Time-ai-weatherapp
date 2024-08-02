SHELL := /bin/bash

# Define the Kafka broker address as a variable
export KAFKA_BROKER_ADDRESS := localhost:19092

run-week:
	source setup_weekly_config.sh && \
	poetry run python src/main.py
run-daily:
	source setup_daily_config.sh && \
	poetry run python src/main.py

run-hourly:
	source setup_hourly_config.sh && \
	poetry run python src/main.py