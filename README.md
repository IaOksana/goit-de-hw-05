# Kafka Sensor Alert Pipeline

A small event-driven pipeline that simulates building sensors and routes abnormal temperature or humidity readings to dedicated Kafka topics.

## Processing flow

1. `admin_part.py` creates the input and alert topics.
2. `producer_part.py` simulates one sensor and publishes readings.
3. `consumer_part.py` evaluates thresholds and produces alert events.
4. `listener_part.py` displays the resulting alerts.

## Setup

1. Create and activate a Python virtual environment.
2. Install dependencies with `pip install -r requirements.txt`.
3. Copy `.env.example` to `.env` and supply valid Kafka connection values.
4. Export the variables from `.env` in your shell or IDE.
5. Run the scripts in the order shown above.

Run `producer_part.py` in multiple terminals to simulate multiple sensors. Set `SENSOR_ID` to keep a chosen identifier; otherwise each process generates one.

## Configuration

Credentials are read from environment variables by `configs.py`. Never commit a populated `.env` file.
