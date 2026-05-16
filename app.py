

from flask import Flask, request, Response
 
import threading
from influxdb_client_3 import InfluxDBClient3, Point


app = Flask(__name__)

# InfluxDB 3 client setup (adjust token, org, and database as needed)
INFLUXDB_URL = "http://localhost:8181"
INFLUXDB_TOKEN = "apiv3_ieEMp9bHiA3PBykkG0pNCyWzTMquZWO67dfBBtlW4ignNgxQsT9q-s2GuU-8yin34-shSHHzktAojjwpJI25TQ"
INFLUXDB_DATABASE = "my_health_db"
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
    print('Received data:', records)

    # Ingest to InfluxDB
    influx_points = []
    for dp in records:
        if dp.get('type') == 'HKQuantityTypeIdentifierHeartRate':
            point = Point("heart_rate") \
                .tag("unit", dp.get("unit", "count/min")) \
                .field("value", dp.get("value", 0))
            # Use startDate as timestamp if present
            if "startDate" in dp:
                point = point.time(dp["startDate"])
            influx_points.append(point)
    if influx_points:
        influx_client.write(points=influx_points)

    return {'status': 'received'}, 200



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True)
