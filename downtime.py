from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
import traceback
from config import INFLUXDB_TOKEN, INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET
import pandas as pd

class DowntimeCalculator:
    def __init__(self):
        self.client = InfluxDBClient(url=INFLUX_URL, token=INFLUXDB_TOKEN, org=INFLUX_ORG)
        self.query_api = self.client.query_api()

    def calculate_downtime_and_connection_lost(self, start_time, end_time, machineId):
        try:
            start_time_str = start_time
            end_time_str = end_time

            raw_query = f'''
                from(bucket: "{INFLUX_BUCKET}")
                    |> range(start: time(v: "{start_time_str}"), stop: time(v: "{end_time_str}"))
                    |> filter(fn: (r) => r["_measurement"] == "machine_strokes")
                    |> filter(fn: (r) => r["_field"] == "totalStrokeCounter")
                    |> filter(fn: (r) => r["machineId"] == "{machineId}")
            '''
            raw_result = self.query_api.query(query=raw_query, org=INFLUX_ORG)

            all_timestamps = []
            for table in raw_result:
                for record in table.records:
                    all_timestamps.append(record.get_time())
            
            if all_timestamps:
                df = pd.DataFrame(all_timestamps, columns=['timestamp'])
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df['diff'] = df['timestamp'].diff().dt.total_seconds()

                connection_lost_periods = []
                connection_lost_duration = 0

                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                if (df['timestamp'].iloc[0] - start_dt).total_seconds() > 300:
                    duration = (df['timestamp'].iloc[0] - start_dt).total_seconds()
                    connection_lost_periods.append({
                        "start_time": start_dt.isoformat().replace('+00:00', 'Z'),
                        "end_time": df['timestamp'].iloc[0].isoformat().replace('+00:00', 'Z'),
                        "duration_seconds": duration
                    })
                    connection_lost_duration += duration

                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                if (end_dt - df['timestamp'].iloc[-1]).total_seconds() > 300:
                    duration = (end_dt - df['timestamp'].iloc[-1]).total_seconds()
                    connection_lost_periods.append({
                        "start_time": df['timestamp'].iloc[-1].isoformat().replace('+00:00', 'Z'),
                        "end_time": end_dt.isoformat().replace('+00:00', 'Z'),
                        "duration_seconds": duration
                    })
                    connection_lost_duration += duration

            downtime_query = f'''
                from(bucket: "{INFLUX_BUCKET}")
                    |> range(start: time(v: "{start_time_str}"), stop: time(v: "{end_time_str}"))
                    |> filter(fn: (r) => r["_measurement"] == "machine_strokes")
                    |> filter(fn: (r) => r["_field"] == "totalStrokeCounter")
                    |> filter(fn: (r) => r["machineId"] == "{machineId}")
                    |> difference(nonNegative: true, columns: ["_value"])
                    |> map(fn: (r) => ({{
                        r with _value: if r._value == 0.0 then 0.0 else 1.0
                    }}))
                    |> yield(name: "status")
            '''

            downtime_result = self.query_api.query(query=downtime_query, org=INFLUX_ORG)

            records = []
            for table in downtime_result:
                for record in table.records:
                    records.append({"time": record.get_time(), "value": record.get_value()})

            df_downtime = pd.DataFrame(records)
            if not df_downtime.empty:
                downtime_slots = []
                total_duration = 0
                downtime_count = 0
                downtime_start = None

                for _, row in df_downtime.iterrows():
                    if row['value'] == 0.0:
                        if downtime_start is None:
                            downtime_start = row['time']

                    else:
                        if downtime_start is not None:
                            downtime_end=row['time']
                            duration = (downtime_end-downtime_start).total_seconds()
                            if duration > 120:
                                downtime_slots.append({
                                    "start_time": downtime_start.isoformat(),
                                    "end_time": downtime_end.isoformat(),
                                    "duration_seconds": duration
                                })
                                total_duration += duration
                                downtime_count += 1
                            downtime_start = None

            return {
                "machineId": machineId,
                "downtime_periods": downtime_slots,
                "total_downtime_duration": total_duration,
                "downtime_count": downtime_count,
                "connection_lost_periods": connection_lost_periods,
                "total_connection_lost_duration": connection_lost_duration,
                "connection_lost_count": len(connection_lost_periods)
            }
        except Exception as e:
            print(f"Error calculating downtime and connection lost: {e}")
            print(traceback.format_exc())
            return {"error": str(e)}

    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
