from flask import Flask, request
import os
from dotenv import load_dotenv
import threading
from influxdb_client_3 import InfluxDBClient3, Point
import psycopg2
from psycopg2.extras import execute_values
from collections import defaultdict
from datetime import datetime

app = Flask(__name__)

# Load environment variables
load_dotenv()

# --- INFLUXDB SETUP ---
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8181")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "")
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", "my_health_db")
influx_client = InfluxDBClient3(host=INFLUXDB_URL, token=INFLUXDB_TOKEN, database=INFLUXDB_DATABASE)

# --- POSTGRESQL SETUP ---
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")
PG_DB = os.getenv("PG_DB", "health")

def get_db_connection():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB)

data_points = []
data_lock = threading.Lock()

@app.route('/healthSync', methods=['POST'])
def health_sync():
    incoming = request.get_json(force=True, silent=True)
    if incoming is None:
        return {'error': 'Invalid JSON'}, 400
        
    if not isinstance(incoming, dict) or 'samples' not in incoming or 'workouts' not in incoming:
        return {'error': "Expected a JSON object with 'samples' and 'workouts' lists"}, 400
        
    samples = incoming['samples']
    workouts = incoming['workouts']
    
    with data_lock:
        data_points.clear()
        data_points.extend(samples)

    # ==========================================
    # 1. POSTGRESQL INGESTION (WORKOUTS)
    # ==========================================
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        for w in workouts:
            # Insert workout (UPSERT to avoid duplicates from 24hr rolling sync)
            # We use DO UPDATE on updated_at purely to force Postgres to RETURN the ID of existing rows
            cur.execute("""
                INSERT INTO workouts (activity_type, duration_seconds, calories_burned, distance_meters, start_date, end_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (activity_type, start_date, end_date) 
                DO UPDATE SET updated_at = NOW()
                RETURNING id;
            """, (w.get('activityType'), w.get('duration'), w.get('calories'), w.get('distance'), w.get('startDate'), w.get('endDate')))
            
            workout_id = cur.fetchone()[0]
            
            # Insert Workout Stats
            for stat in w.get('stats', []):
                cur.execute("""
                    INSERT INTO workout_stats (workout_id, stat_type, stat_average, stat_sum, stat_min, stat_max, unit)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (workout_id, stat_type) 
                    DO UPDATE SET stat_average = EXCLUDED.stat_average;
                """, (workout_id, stat.get('type'), stat.get('average'), stat.get('sum'), stat.get('min'), stat.get('max'), stat.get('unit')))
            
            # Insert Workout Splits (If they exist in the payload)
            for split in w.get('splits', []):
                cur.execute("""
                    INSERT INTO workout_splits (workout_id, split_number, duration_seconds, avg_heart_rate, avg_pace_seconds_per_km)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (workout_id, split_number) DO NOTHING;
                """, (workout_id, split.get('kilometer'), split.get('durationSeconds'), split.get('avgHeartRate'), split.get('avgPaceSecondsPerKm')))

        conn.commit()
        cur.close()
        conn.close()
        print(f"Successfully processed {len(workouts)} workouts into PostgreSQL.")
        
    except Exception as e:
        print(f"FAILED to write to PostgreSQL: {e}")


    # ==========================================
    # 2. INFLUXDB INGESTION (SAMPLES)
    # ==========================================
    influx_points = []
    
    # Raw Heart Rate points
    for dp in samples:
        if dp.get('type') == 'HKQuantityTypeIdentifierHeartRate':
            point = Point("heart_rate") \
                .tag("unit", dp.get("unit", "count/min")) \
                .field("value", int(float(dp.get("value", 0))))
            if "startDate" in dp:
                point = point.time(dp["startDate"])
            influx_points.append(point)

    # Step count and Energy aggregation per hour
    step_counts_by_hour = defaultdict(int)
    energy_by_hour = defaultdict(float)
    
    for dp in samples:
        start = dp.get('startDate')
        if not start:
            continue
        # Ensure correct ISO format parsing for Python
        dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
        dt_hour = dt.replace(minute=0, second=0, microsecond=0)
        
        if dp.get('type') == 'HKQuantityTypeIdentifierStepCount':
            step_counts_by_hour[dt_hour] += int(float(dp.get('value', 0)))
        elif dp.get('type') == 'HKQuantityTypeIdentifierActiveEnergyBurned':
            energy_by_hour[dt_hour] += float(dp.get('value', 0))

    for hour, total in step_counts_by_hour.items():
        point = Point("step_count").tag("unit", "count").field("value", total).time(hour.isoformat())
        influx_points.append(point)

    for hour, total in energy_by_hour.items():
        point = Point("active_energy_burnt").tag("unit", "kcal").field("value", int(total)).time(hour.isoformat())
        influx_points.append(point)

    try:
        if influx_points:
            influx_client.write(record=influx_points)
            print(f"Successfully wrote {len(influx_points)} points to InfluxDB.")
    except Exception as e:
        print(f"FAILED to write to InfluxDB: {e}")

    return {'status': 'received'}, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True)