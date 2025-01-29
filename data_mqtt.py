
from config import MQTT_BROKER, MQTT_PORT, MQTT_TOPIC #type:ignore


import logging
import json
from mqtt_handler import MQTTHandler #type:ignore

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    logger.info(f"Received message: {payload}")

if __name__ == "__main__":
    mqtt_handler = MQTTHandler(MQTT_BROKER, MQTT_PORT, MQTT_TOPIC, on_message)

    try:
        logger.info("Starting MQTT listener...")
        mqtt_handler.connect()
        mqtt_handler.start_loop()
        
        while True:
            pass
            
    except KeyboardInterrupt:
        logger.info("Stopping MQTT listener...")
    finally:
        mqtt_handler.stop_loop()


import paho.mqtt.client as mqtt #type:ignore
import logging

logger = logging.getLogger(__name__)

class MQTTHandler:
    def __init__(self, broker, port, topic, on_message_callback):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = on_message_callback

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to MQTT broker")
            self.mqtt_client.subscribe(self.topic)
            logger.info(f"Subscribed to topic: {self.topic}")

    def connect(self):
        self.mqtt_client.connect(self.broker, self.port, 60)

    def start_loop(self):
        self.mqtt_client.loop_start()

    def stop_loop(self):
        self.mqtt_client.loop_stop()