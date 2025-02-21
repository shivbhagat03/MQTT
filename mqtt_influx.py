import paho.mqtt.client as mqtt #type:ignore
import json
from influx_handler import InfluxHandler
from config import MQTT_BROKER, MQTT_PORT, MQTT_TOPIC, INFLUX_URL, INFLUXDB_TOKEN, INFLUX_ORG, INFLUX_BUCKET


influx_handler = InfluxHandler(
    url=INFLUX_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUX_ORG,
    bucket=INFLUX_BUCKET
)

def on_connect(client, userdata, flags, rc):
    
    print(f"Connected to MQTT broker with result code {rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        
        payload = json.loads(msg.payload.decode()) 
        
        
        machineId = payload.get('machineId')
        stroke_count = payload.get('totalStrokeCounter')
        timestamp = payload.get('time')
        
        if all([machineId, stroke_count, timestamp]):
            
            influx_handler.write_data(
                measurement="machine_strokes",
                machineId=machineId,
                stroke_count=stroke_count,
                timestamp=timestamp
            )
        else:
            print("Missing required fields in message")
            
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON message: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    try:
        
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message

        client.connect(MQTT_BROKER, MQTT_PORT, 60)

        
        print("Starting MQTT loop...")
        client.loop_forever()
        
    except KeyboardInterrupt:
        print("\nShutting down...")
        influx_handler.close()
    except Exception as e:
        print(f"Error in main loop: {e}")
        influx_handler.close()

if __name__ == "__main__":
    main()