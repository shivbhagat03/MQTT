from influxdb_client import InfluxDBClient
import datetime
from config import INFLUXDB_TOKEN, INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET

class ProductionCalculator:
    def __init__(self):
        self.client = InfluxDBClient(url=INFLUX_URL, token=INFLUXDB_TOKEN)
        self.query_api = self.client.query_api()

    def calculate_production(self, start_time, end_time, machineId):
        try: 
            start = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").isoformat() + "Z"
            end = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").isoformat() + "Z"

            query = f'''
                first_value = from(bucket: "{INFLUX_BUCKET}")
                    |> range(start: {start}, stop: {end})
                    |> filter(fn: (r) => r._measurement == "machine_strokes")
                    |> filter(fn: (r) => r.machineId == "{machineId}")
                    |> filter(fn: (r) => r._field == "totalStrokeCounter")
                    |> first()

                last_value = from(bucket: "{INFLUX_BUCKET}")
                    |> range(start: {start}, stop: {end})
                    |> filter(fn: (r) => r._measurement == "machine_strokes")
                    |> filter(fn: (r) => r.machineId == "{machineId}")
                    |> filter(fn: (r) => r._field == "totalStrokeCounter")
                    |> last()

                join(tables: {{first: first_value, last: last_value}}, on: ["_field"])
                    |> map(fn: (r) => ({{
                        machineId: "{machineId}",
                        production_value: r._value_last - r._value_first
                    }}))
            '''

            result = self.query_api.query(query, org=INFLUX_ORG)

            if result and len(result) > 0 and len(result[0].records) > 0:
                record = result[0].records[0]
                return {
                    "machineId": machineId,    
                    "start_time": start_time,
                    "end_time": end_time,
                    "production_value": record.values.get("production_value", 0)
                }
            else:
                return {
                    "error": "No data found for the specified time range and machine ID"
                }

        except Exception as e:
            return {
                "error": f"Error calculating production value: {str(e)}"
            }

    def close(self):
        self.client.close()
