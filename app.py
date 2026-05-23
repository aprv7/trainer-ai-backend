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


# --- DASHBOARD ENDPOINT AND HELPERS ---
from flask import jsonify
from datetime import timedelta

def get_activity_type_map(cur):
    cur.execute("SELECT id, name FROM workout_activity_types")
    return {row[0]: row[1] for row in cur.fetchall()}

def get_heart_rate_zones(hr):
    zones = [
        {"zone": "Z1 Recovery", "min": 0, "max": 120, "color": "#6ee7b7"},
        {"zone": "Z2 Aerobic", "min": 121, "max": 140, "color": "#34d399"},
        {"zone": "Z3 Tempo", "min": 141, "max": 160, "color": "#fbbf24"},
        {"zone": "Z4 Threshold", "min": 161, "max": 180, "color": "#fb923c"},
        {"zone": "Z5 VO2max", "min": 181, "max": 300, "color": "#f87171"},
    ]
    zone_minutes = {z["zone"]: 0 for z in zones}
    for hr_val, duration in hr:
        for z in zones:
            if z["min"] <= hr_val <= z["max"]:
                zone_minutes[z["zone"]] += duration
                break
    return [
        {"zone": z["zone"], "minutes": int(zone_minutes[z["zone"]]), "color": z["color"]}
        for z in zones
    ]

@app.route('/dashboard', methods=['GET'])
def get_dashboard():
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        activity_type_map = get_activity_type_map(cur)

        # --- Recent Workouts (last 7 days, sorted desc) ---
        cur.execute("""
            SELECT id, activity_type, duration_seconds, calories_burned, distance_meters, start_date, end_date
            FROM workouts
            WHERE start_date >= %s
            ORDER BY start_date DESC
        """, (week_ago,))
        workouts = cur.fetchall()

        # --- Weekly Summary ---
        total_distance = 0
        total_calories = 0
        workout_count = len(workouts)
        paces = []
        avg_hrs = []
        longest_run = 0
        workout_objs = []
        all_dates = set()

        for w in workouts:
            wid, atype, dur, cal, dist, sdate, edate = w
            all_dates.add(sdate.date())
            # Get stats for this workout
            cur.execute("SELECT stat_type, stat_average, stat_sum, stat_min, stat_max, unit FROM workout_stats WHERE workout_id=%s", (wid,))
            stats = {row[0]: row for row in cur.fetchall()}
            # Get splits
            cur.execute("SELECT split_number, duration_seconds, avg_heart_rate, avg_pace_seconds_per_km FROM workout_splits WHERE workout_id=%s ORDER BY split_number", (wid,))
            splits = cur.fetchall()
            # --- Build splits for API ---
            split_objs = []
            for s in splits:
                snum, sdur, shr, space = s
                split_obj = {"km": snum}
                if space is not None:
                    split_obj["paceMinPerKm"] = round(space/60, 2)
                if shr is not None:
                    split_obj["heartRate"] = round(shr)
                if sdur is not None:
                    split_obj["durationMin"] = round(sdur/60, 2)
                split_objs.append(split_obj)

            # --- Build workout object ---
            workout_obj = {
                "id": f"w-{wid}",
                "date": sdate.date().isoformat(),
                "title": activity_type_map.get(atype, "Workout"),
                "type": activity_type_map.get(atype, "Workout").lower(),
                "distanceKm": round(dist/1000, 2) if dist else 0,
                "durationMin": round(dur/60, 2) if dur else 0,
                "avgHeartRate": round(float(stats.get("HKQuantityTypeIdentifierHeartRate", [None, None, 0])[2] or 0)),
                "maxHeartRate": round(float(stats.get("HKQuantityTypeIdentifierHeartRate", [None, None, None, None, stats.get("HKQuantityTypeIdentifierHeartRate", [None]*5)[4]])[4] or 0)),
                "calories": round(cal or 0),
                "splits": split_objs
            }
            workout_objs.append(workout_obj)

            # --- Weekly summary aggregation ---
            if activity_type_map.get(atype, "").lower() == "running":
                total_distance += dist or 0
                if dist and dist > longest_run:
                    longest_run = dist
            total_calories += cal or 0
            # Pace (min/km)
            if dist and dist > 0 and dur and activity_type_map.get(atype, "").lower() == "running":
                pace = (dur/60) / (dist/1000)
                paces.append(pace)
            # Avg HR
            if "HKQuantityTypeIdentifierHeartRate" in stats and stats["HKQuantityTypeIdentifierHeartRate"][2]:
                avg_hrs.append(float(stats["HKQuantityTypeIdentifierHeartRate"][2]))

        # --- Daily Metrics (distance, calories, avgHR per day) ---
        daily_metrics = []
        for i in range(7):
            day = (week_ago + timedelta(days=i)).date()
            cur.execute("""
                SELECT SUM(distance_meters), SUM(calories_burned), AVG(ws.stat_average)
                FROM workouts w
                LEFT JOIN workout_stats ws ON w.id = ws.workout_id AND ws.stat_type = 'HKQuantityTypeIdentifierHeartRate'
                WHERE w.start_date >= %s AND w.start_date < %s AND w.start_date::date = %s
            """, (week_ago, now, day))
            dist, cal, avghr = cur.fetchone()
            daily_metrics.append({
                "date": day.isoformat(),
                "distanceKm": round((dist or 0)/1000, 2),
                "calories": round(cal or 0),
                "avgHeartRate": round(avghr or 0) if avghr else 0
            })

        # --- Heart Rate Zones (aggregate all splits HR by duration) ---
        cur.execute("""
            SELECT avg_heart_rate, duration_seconds FROM workout_splits ws
            JOIN workouts w ON ws.workout_id = w.id
            WHERE w.start_date >= %s
        """, (week_ago,))
        hr_zone_data = [(float(hr or 0), float(dur or 0)/60) for hr, dur in cur.fetchall() if hr and dur]
        heart_rate_zones = get_heart_rate_zones(hr_zone_data)

        # --- Weekly Summary ---
        weekly_summary = {
            "totalDistanceKm": round(total_distance/1000, 2),
            "totalCalories": round(total_calories),
            "workoutCount": workout_count,
            "avgPaceMinPerKm": round(sum(paces)/len(paces), 2) if paces else 0,
            "avgHeartRate": round(sum(avg_hrs)/len(avg_hrs)) if avg_hrs else 0,
            "longestRunKm": round(longest_run/1000, 2) if longest_run else 0
        }

        # --- Workout Plan (stub, as not enough info) ---
        workout_plan = {
            "weekOf": week_ago.date().isoformat(),
            "summary": "Build aerobic base with one quality session.",
            "rationale": f"Last week was {weekly_summary['totalDistanceKm']} km with elevated tempo HR…",
            "sessions": []
        }

        response = {
            "lastUpdated": now.replace(microsecond=0).isoformat() + "Z",
            "weeklySummary": weekly_summary,
            "dailyMetrics": daily_metrics,
            "recentWorkouts": workout_objs,
            "heartRateZones": heart_rate_zones,
            "workoutPlan": workout_plan
        }
        cur.close()
        conn.close()
        return jsonify(response)
    except Exception as e:
        print(f"/dashboard error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True)


# --- DASHBOARD ENDPOINT ---
from flask import jsonify
from datetime import timedelta

def get_activity_type_map(cur):
    cur.execute("SELECT id, name FROM workout_activity_types")
    return {row[0]: row[1] for row in cur.fetchall()}

def get_heart_rate_zones(hr):
    # Standard running HR zones (example, adjust as needed)
    # Z1: <60% max, Z2: 60-70%, Z3: 70-80%, Z4: 80-90%, Z5: >90%
    # We'll use color and names as in your spec
    # This function expects a list of (hr, duration_min) tuples
    zones = [
        {"zone": "Z1 Recovery", "min": 0, "max": 120, "color": "#6ee7b7"},
        {"zone": "Z2 Aerobic", "min": 121, "max": 140, "color": "#34d399"},
        {"zone": "Z3 Tempo", "min": 141, "max": 160, "color": "#fbbf24"},
        {"zone": "Z4 Threshold", "min": 161, "max": 180, "color": "#fb923c"},
        {"zone": "Z5 VO2max", "min": 181, "max": 300, "color": "#f87171"},
    ]
    zone_minutes = {z["zone"]: 0 for z in zones}
    for hr_val, duration in hr:
        for z in zones:
            if z["min"] <= hr_val <= z["max"]:
                zone_minutes[z["zone"]] += duration
                break
    return [
        {"zone": z["zone"], "minutes": int(zone_minutes[z["zone"]]), "color": z["color"]}
        for z in zones
    ]

@app.route('/dashboard', methods=['GET'])
def get_dashboard():
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        activity_type_map = get_activity_type_map(cur)

        # --- Recent Workouts (last 7 days, sorted desc) ---
        cur.execute("""
            SELECT id, activity_type, duration_seconds, calories_burned, distance_meters, start_date, end_date
            FROM workouts
            WHERE start_date >= %s
            ORDER BY start_date DESC
        """, (week_ago,))
        workouts = cur.fetchall()

        # --- Weekly Summary ---
        total_distance = 0
        total_calories = 0
        workout_count = len(workouts)
        paces = []
        avg_hrs = []
        longest_run = 0
        workout_objs = []
        all_dates = set()

        for w in workouts:
            wid, atype, dur, cal, dist, sdate, edate = w
            all_dates.add(sdate.date())
            # Get stats for this workout
            cur.execute("SELECT stat_type, stat_average, stat_sum, stat_min, stat_max, unit FROM workout_stats WHERE workout_id=%s", (wid,))
            stats = {row[0]: row for row in cur.fetchall()}
            # Get splits
            cur.execute("SELECT split_number, duration_seconds, avg_heart_rate, avg_pace_seconds_per_km FROM workout_splits WHERE workout_id=%s ORDER BY split_number", (wid,))
            splits = cur.fetchall()
            # --- Build splits for API ---
            split_objs = []
            for s in splits:
                snum, sdur, shr, space = s
                split_obj = {"km": snum}
                if space is not None:
                    split_obj["paceMinPerKm"] = round(space/60, 2)
                if shr is not None:
                    split_obj["heartRate"] = round(shr)
                if sdur is not None:
                    split_obj["durationMin"] = round(sdur/60, 2)
                split_objs.append(split_obj)

            # --- Build workout object ---
            workout_obj = {
                "id": f"w-{wid}",
                "date": sdate.date().isoformat(),
                "title": activity_type_map.get(atype, "Workout"),
                "type": activity_type_map.get(atype, "Workout").lower(),
                "distanceKm": round(dist/1000, 2) if dist else 0,
                "durationMin": round(dur/60, 2) if dur else 0,
                "avgHeartRate": round(float(stats.get("HKQuantityTypeIdentifierHeartRate", [None, None, 0])[2] or 0)),
                "maxHeartRate": round(float(stats.get("HKQuantityTypeIdentifierHeartRate", [None, None, None, None, stats.get("HKQuantityTypeIdentifierHeartRate", [None]*5)[4]])[4] or 0)),
                "calories": round(cal or 0),
                "splits": split_objs
            }
            workout_objs.append(workout_obj)

            # --- Weekly summary aggregation ---
            if activity_type_map.get(atype, "").lower() == "running":
                total_distance += dist or 0
                if dist and dist > longest_run:
                    longest_run = dist
            total_calories += cal or 0
            # Pace (min/km)
            if dist and dist > 0 and dur and activity_type_map.get(atype, "").lower() == "running":
                pace = (dur/60) / (dist/1000)
                paces.append(pace)
            # Avg HR
            if "HKQuantityTypeIdentifierHeartRate" in stats and stats["HKQuantityTypeIdentifierHeartRate"][2]:
                avg_hrs.append(float(stats["HKQuantityTypeIdentifierHeartRate"][2]))

        # --- Daily Metrics (distance, calories, avgHR per day) ---
        daily_metrics = []
        for i in range(7):
            day = (week_ago + timedelta(days=i)).date()
            cur.execute("""
                SELECT SUM(distance_meters), SUM(calories_burned), AVG(ws.stat_average)
                FROM workouts w
                LEFT JOIN workout_stats ws ON w.id = ws.workout_id AND ws.stat_type = 'HKQuantityTypeIdentifierHeartRate'
                WHERE w.start_date >= %s AND w.start_date < %s AND w.start_date::date = %s
            """, (week_ago, now, day))
            dist, cal, avghr = cur.fetchone()
            daily_metrics.append({
                "date": day.isoformat(),
                "distanceKm": round((dist or 0)/1000, 2),
                "calories": round(cal or 0),
                "avgHeartRate": round(avghr or 0) if avghr else 0
            })

        # --- Heart Rate Zones (aggregate all splits HR by duration) ---
        cur.execute("""
            SELECT avg_heart_rate, duration_seconds FROM workout_splits ws
            JOIN workouts w ON ws.workout_id = w.id
            WHERE w.start_date >= %s
        """, (week_ago,))
        hr_zone_data = [(float(hr or 0), float(dur or 0)/60) for hr, dur in cur.fetchall() if hr and dur]
        heart_rate_zones = get_heart_rate_zones(hr_zone_data)

        # --- Weekly Summary ---
        weekly_summary = {
            "totalDistanceKm": round(total_distance/1000, 2),
            "totalCalories": round(total_calories),
            "workoutCount": workout_count,
            "avgPaceMinPerKm": round(sum(paces)/len(paces), 2) if paces else 0,
            "avgHeartRate": round(sum(avg_hrs)/len(avg_hrs)) if avg_hrs else 0,
            "longestRunKm": round(longest_run/1000, 2) if longest_run else 0
        }

        # --- Workout Plan (stub, as not enough info) ---
        workout_plan = {
            "weekOf": week_ago.date().isoformat(),
            "summary": "Build aerobic base with one quality session.",
            "rationale": f"Last week was {weekly_summary['totalDistanceKm']} km with elevated tempo HR…",
            "sessions": []
        }

        response = {
            "lastUpdated": now.replace(microsecond=0).isoformat() + "Z",
            "weeklySummary": weekly_summary,
            "dailyMetrics": daily_metrics,
            "recentWorkouts": workout_objs,
            "heartRateZones": heart_rate_zones,
            "workoutPlan": workout_plan
        }
        cur.close()
        conn.close()
        return jsonify(response)
    except Exception as e:
        print(f"/dashboard error: {e}")
        return jsonify({"error": str(e)}), 500