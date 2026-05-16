

from flask import Flask, request, Response
import os
from dotenv import load_dotenv
 
import threading
from influxdb_client_3 import InfluxDBClient3, Point


app = Flask(__name__)

# Load environment variables from .env file
load_dotenv()

# InfluxDB 3 client setup using environment variables
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8181")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "")
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", "my_health_db")
influx_client = InfluxDBClient3(
    host=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    database=INFLUXDB_DATABASE
)

# In-memory storage for metrics
data_points = []
data_lock = threading.Lock()

@app.route('/healthSync', methods=['POST'])
def health_sync():
    print(request.data)
    incoming = request.get_json(force=True, silent=True)
    if incoming is None:
        return {'error': 'Invalid JSON'}, 400
    # Expecting a dict with 'records' key containing a list
    if not isinstance(incoming, dict) or 'records' not in incoming or not isinstance(incoming['records'], list):
        return {'error': "Expected a JSON object with a 'records' key containing a list of data points"}, 400
    records = incoming['records']
    with data_lock:
        data_points.clear()
        data_points.extend(records)

    # Ingest to InfluxDB

    influx_points = []
    # Heart rate points
    for dp in records:
        print(dp)
        if dp.get('type') == 'HKQuantityTypeIdentifierHeartRate':
            point = Point("heart_rate") \
                .tag("unit", dp.get("unit", "count/min")) \
                .field("value", int(float(dp.get("value", 0))))
            if "startDate" in dp:
                point = point.time(dp["startDate"])
            influx_points.append(point)


    # Step count aggregation per hour
    from collections import defaultdict
    from datetime import datetime

    step_counts_by_hour = defaultdict(int)
    energy_by_hour = defaultdict(float)
    for dp in records:
        start = dp.get('startDate')
        if not start:
            continue
        dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
        dt_hour = dt.replace(minute=0, second=0, microsecond=0)
        if dp.get('type') == 'HKQuantityTypeIdentifierStepCount':
            step_counts_by_hour[dt_hour] += int(float(dp.get('value', 0)))
        elif dp.get('type') == 'HKQuantityTypeIdentifierActiveEnergyBurned':
            energy_by_hour[dt_hour] += float(dp.get('value', 0))

    for hour, total in step_counts_by_hour.items():
        point = Point("step_count") \
            .tag("unit", "count") \
            .field("value", total) \
            .time(hour.isoformat())
        influx_points.append(point)

    for hour, total in energy_by_hour.items():
        point = Point("active_energy_burnt") \
            .tag("unit", "kcal") \
            .field("value", int(total)) \
            .time(hour.isoformat())
        influx_points.append(point)

    try:
        if influx_points:
            influx_client.write(record=influx_points)
            print(f"Successfully wrote {len(influx_points)} points to InfluxDB")
    except Exception as e:
        print(f"FAILED to write to InfluxDB: {e}")

    return {'status': 'received'}, 200



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True)
