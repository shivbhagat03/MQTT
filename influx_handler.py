from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime
from config import INFLUXDB_TOKEN, INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET

class InfluxHandler:
    def __init__(self, url="INFLUX_URL", token="INFLUXDB_TOKEN", org="INFLUX_ORG", bucket="INFLUX_BUCKET"):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.bucket = bucket
        self.org = org
        
        try:
            health = self.client.health()
            print(f"Connected to InfluxDB! Status: {health.status}")
        except Exception as e:
            print(f"Failed to connect to InfluxDB: {e}")

    def write_data(self, measurement, machineId, stroke_count, timestamp):
        try:
            
            ist_time = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
            utc_time = ist_time.astimezone(datetime.timezone.utc)
            point = Point(measurement) \
                .tag("machineId", machineId) \
                .field("totalStrokeCounter", stroke_count) \
                .time(utc_time)

            
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            print(f"Successfully wrote data point: Machine ID={machineId}, Stroke Count={stroke_count}, Time={utc_time}")
            
            
            query = f'''
                from(bucket: "{self.bucket}")
                |> range(start: -1m)
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> filter(fn: (r) => r["machineId"] == "{machineId}")
                |> last()
            '''
            result = self.client.query_api().query(query, org=self.org)
            if len(result) > 0:
                print("Data verified in database!")
            
        except Exception as e:
            print(f"Error writing to InfluxDB: {e}")

    def close(self):
        self.client.close()  

