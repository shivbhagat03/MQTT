import paho.mqtt.client as mqtt  #type: ignore
import json
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
            logger.info(f"Connected to MQTT broker at {self.broker}")
            self.mqtt_client.subscribe(self.topic)
            logger.info(f"Subscribed to topic: {self.topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker with code: {rc}")

    def connect(self):
        self.mqtt_client.connect(self.broker, self.port, 60)

    def start_loop(self):
        self.mqtt_client.loop_start()

    def stop_loop(self):
        self.mqtt_client.loop_stop()

    def publish(self, topic, message):
        self.mqtt_client.publish(topic, json.dumps(message))
        logger.info(f"Published to topic {topic}: {message}")
