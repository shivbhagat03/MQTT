from flask import Flask, jsonify, request
from datetime import datetime
from config import INFLUXDB_TOKEN, INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET
import traceback
from downtime import DowntimeCalculator

app = Flask(__name__)
calculator = DowntimeCalculator()

@app.route("/api/downtime", methods=["GET"])
def get_production_status():
    try:
        machineId = request.args.get('machineId', '').strip()
        start_time = request.args.get('start', '').strip()
        stop_time = request.args.get('stop', '').strip()

        if not all([machineId, start_time, stop_time]):
            return jsonify({"error": "Missing required parameters"}), 400
        
        try:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            stop_dt = datetime.fromisoformat(stop_time.replace('Z', '+00:00'))

            influx_start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            influx_stop = stop_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError as e:
            return jsonify({"error": f"Invalid datetime format: {str(e)}"}), 400

        result = calculator.calculate_downtime_and_connection_lost(influx_start, influx_stop, machineId)

        if "error" in result:
            return jsonify({"error": result["error"]}), 500

        response = {
            "machineId": machineId,
            "timeRange": {
                "start": start_time,
                "stop": stop_time
            },
            "downtime": {
                "periods": result.get("downtime_periods", []),
                "totalDuration": result.get("total_downtime_duration", 0),
                "count": result.get("downtime_count", 0)
            },
            "connectionLost": {
                "periods": result.get("connection_lost_periods", []),
                "totalDuration": result.get("total_connection_lost_duration", 0),
                "count": result.get("connection_lost_count", 0)
            }
        }

        return jsonify(response), 200

    except Exception as e:
        print(f"Error in API: {str(e)}")
        print(traceback.format_exc())
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8080, debug=True)
