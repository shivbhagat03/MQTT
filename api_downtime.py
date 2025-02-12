from flask import Flask, jsonify, request
from datetime import datetime
from config import INFLUXDB_TOKEN, INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET
import traceback
from downtime import DowntimeCalculator  

app = Flask(__name__)
calculator = DowntimeCalculator()  

@app.route("/api/downtime", methods=["GET"])
def get_downtime():
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

        result = calculator.calculate_downtime_slots(influx_start, influx_stop, machineId)

        response = {
            "machineId": machineId,
            "timeRange": {
                "start": start_time,
                "stop": stop_time
            },
            "downtimePeriods": result.get("downtime_periods", []),
            "totalduration": result.get("total_duration", 0),
            "totalcount": result.get("downtime_count", 0)
        }

        return jsonify(response), 200 if not result.get("error") else 500

    except Exception as e:
        print(f"Error in API: {str(e)}")
        print(traceback.format_exc())
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8080, debug=True)
