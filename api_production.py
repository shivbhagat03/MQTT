from flask import Flask, jsonify, request
from influxdb_client import InfluxDBClient
from werkzeug.serving import make_server
import threading
from datetime import datetime
from config import INFLUXDB_TOKEN, INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET
from productionval import ProductionCalculator

app = Flask(__name__)

calculator = ProductionCalculator()

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "message": "Production API is running",
        "endpoints": {
            "fetch_data": "/fetch_data?machineId=<machine_id>&start=<iso_datetime>&stop=<iso_datetime>"
        }
    })

@app.route("/api/production", methods=["GET"])
def get_production():
    machine_id = request.args.get('machineId')
    start_time = request.args.get('start')
    end_time = request.args.get('stop')

    start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
    end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                
    new_start = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    new_end= end_dt.strftime("%Y-%m-%d %H:%M:%S")

    result = calculator.calculate_production(new_start, new_end, machine_id)

    if "error" in result:
        return jsonify(result), 404
    
    return jsonify(result), 200

def run_app():
    app.run(host='127.0.0.1', port=5000, debug=False)

if __name__ == "__main__":
    run_app()