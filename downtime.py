from influxdb_client import InfluxDBClient
from datetime import datetime
from flask import Flask, jsonify, request
import traceback
from config import INFLUXDB_TOKEN, INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET

app = Flask(__name__)

class DowntimeCalculator:
    def __init__(self):
        self.client = InfluxDBClient(url=INFLUX_URL, token=INFLUXDB_TOKEN, org=INFLUX_ORG)
        self.query_api = self.client.query_api()

    def calculate_downtime_slots(self, start_time, end_time, machineId):
        try:
            
            if isinstance(start_time, datetime):
                start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                start_time_str = start_time

            if isinstance(end_time, datetime):
                end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                end_time_str = end_time

            query = f'''
                from(bucket: "{INFLUX_BUCKET}")
                    |> range(start: time(v: "{start_time_str}"), stop: time(v: "{end_time_str}"))
                    |> filter(fn: (r) => r["_measurement"] == "machine_strokes")
                    |> filter(fn: (r) => r["_field"] == "totalStrokeCounter")
                    |> filter(fn: (r) => r["machineId"] == "{machineId}")
                    |> difference(nonNegative: true, columns: ["_value"])
                    |> aggregateWindow(every: 10s ,fn:mean)
                    |> map(fn: (r) => ({{
                        r with _value: if r._value == 0.0 then 0.0 else 1.0
                    }}))
                    |> yield(name: "status")
            '''  

            result = self.query_api.query(query=query, org=INFLUX_ORG)

            records = []
            for table in result:
                for record in table.records:
                    records.append({"time": record.get_time(), "_value": record.get_value()})

            downtime_slots = []
            downtime_start = None
            total_duration=0
            downtime_count=0

            for row in records:
                if row["_value"] == 0.0:
                    if downtime_start is None:
                        downtime_start = row["time"]
                else:
                    if downtime_start is not None:
                        downtime_end = row["time"]
                        duration = (downtime_end - downtime_start).total_seconds()
                        if duration > 120:
                            downtime_slots.append({
                                "start_time": downtime_start.isoformat(),
                                "end_time": downtime_end.isoformat(),
                                "duration_seconds": duration
                            })
                            total_duration +=duration
                            downtime_count +=1
                        downtime_start = None

            return {"machineId": machineId, "downtime_periods": downtime_slots,"total_duration":total_duration,"downtime_count":downtime_count}

        except Exception as e:
            print(f"Error calculating downtime slots: {e}")
            print(traceback.format_exc())
            return {"error": str(e)}

    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
