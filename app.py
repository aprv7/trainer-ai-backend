
from flask import Flask, request, Response
from prometheus_client import CollectorRegistry, Gauge, generate_latest, CONTENT_TYPE_LATEST
import threading

app = Flask(__name__)

# In-memory storage for metrics
data_points = []
data_lock = threading.Lock()

@app.route('/healthSync', methods=['POST'])
def health_sync():
    incoming = request.get_json(force=True, silent=True)
    if incoming is None:
        return {'error': 'Invalid JSON'}, 400
    # Expecting a list of dicts
    if not isinstance(incoming, list):
        return {'error': 'Expected a list of data points'}, 400
    with data_lock:
        data_points.clear()
        data_points.extend(incoming)
    print('Received data:', incoming)
    return {'status': 'received'}, 200

@app.route('/metrics')
def metrics():
    registry = CollectorRegistry()
    heart_rate_gauge = Gauge('apple_healthkit_heart_rate', 'Heart rate from Apple HealthKit', ['startDate', 'endDate', 'unit'], registry=registry)
    with data_lock:
        for dp in data_points:
            if dp.get('type') == 'HKQuantityTypeIdentifierHeartRate':
                heart_rate_gauge.labels(
                    startDate=dp.get('startDate', ''),
                    endDate=dp.get('endDate', ''),
                    unit=dp.get('unit', 'count/min')
                ).set(dp.get('value', 0))
    return Response(generate_latest(registry), mimetype=CONTENT_TYPE_LATEST)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True)
