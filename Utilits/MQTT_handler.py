# -- coding: utf-8 --
from __future__ import unicode_literals
import paho.mqtt.client as mqtt
from multiprocessing import Queue
from Utilits.log_settings import log


class MqttClient:
    def __init__(self, topic):
        self.client = mqtt.Client(protocol=mqtt.MQTTv31)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.topic = topic

        self.connected_rc = None
        self.msg_queue = Queue()

    def mqtt_connect(self, url, port):
        try:
            self.client.connect(url, port=port, keepalive=60)
        except:
            return 0
        else:
            self.client.loop_start()
            return 1

    def on_connect(self, client, userdata, flags, rc):
        self.connected_rc = rc
        if self.connected_rc:
            log.info("connection error code: {}".format(str(self.connected_rc)))
        else:
            client.subscribe("{}/#".format(self.topic))

    def on_message(self, client, userdata, msg):
        self.msg_queue.put(msg.payload)

    def on_subscribe(self, client, userdata, mid, granted_qos):
        pass

    def publish(self, topic, msg):
        self.client.publish(topic, msg)

    def get_rx_msg(self, timeout):
        return self.msg_queue.get(block=True, timeout=timeout)







