# -- coding: utf-8 --
from __future__ import unicode_literals
from threading import Thread
from Utilits import MQTT_handler
from time import sleep
import collections
from Utilits.log_settings import log
import json
import time
from Utilits.CommonQueue import CommonQueue


class Worker(Thread):
    def __init__(self, ip, sn, cycles):
        super().__init__()
        self.lost_command = 0
        self.common_count = 0
        self.lost_node_sc_data = 0
        self.good = 0
        self.bad = 0
        self.wait = True
        self.break_test = False
        self.__cycles = cycles
        self.__ip = ip
        self.__device = sn
        self.__topic_read = '/field/rf/valve/{}'.format(sn)
        self.__topic_write = '/central/rf/valve/{}'.format(sn)
        self.__object = MQTT_handler.MqttClient(topic=self.__topic_read)

        self.__command = {"command": "control_logic", "id": sn, "port": 1, "value": 2, "wor": True}

    def __str__(self):
        return '{}'.format(self.__device)

    def __set_up(self):
        if self.__conn():
            self.__get_heartbeat()
        else:
            CommonQueue.SysCQ.put({'Error': True, 'Event': 'Disconnected', 'Object': str(self)}, block=False)

    def __conn(self):
        for i in range(5):
            if self.__object.mqtt_connect(self.__ip, 1883):
                return True
            sleep(0.7)
        return False

    def __get_heartbeat(self):
        log.info('{}: Waiting a heartbeat...'.format(str(self)))
        rsp = Worker.__wait_mqtt_rsp(cls=self.__object,
                                     keys=['value', 'sequence'],
                                     timeout_msec=70000,
                                     sub_command='node_sc_heartbeat')

        if rsp:
            if rsp['value'] == 1 or rsp['value'] == 2:
                self.__command['value'] = 2 if rsp['value'] == 1 else 1
                CommonQueue.SysCQ.put({'Error': False, 'Event': 'Connected', 'Object': str(self)}, block=False)
            elif rsp['value'] == 3:
                CommonQueue.SysCQ.put({'Error': True, 'Event': 'Test Failed. Pipe disconnected', 'Object': str(self)}, block=False)
            else:
                CommonQueue.SysCQ.put({'Error': True, 'Event': 'Test Failed. Slider in unknown position', 'Object': str(self)}, block=False)
        else:
            CommonQueue.SysCQ.put({'Error': True, 'Event': 'Test Failed. Lost device', 'Object': str(self)}, block=False)

    def __send_command(self):
        self.__object.publish(self.__topic_write, json.dumps(self.__command))
        rsp = Worker.__wait_mqtt_rsp(cls=self.__object,
                                     keys=['value', 'sequence'],
                                     timeout_msec=10000,
                                     sub_command='node_sc_data')
        log.info('{}: {}'.format(str(self), rsp))
        if rsp:
            if rsp['value'] == 1 or rsp['value'] == 2:
                if rsp['value'] == self.__command['value']:
                    self.__command['value'] = 2 if rsp['value'] == 1 else 1
                    self.good += 1
                    CommonQueue.SysCQ.put({'Error': False, 'Event': 'Good', 'Object': str(self)}, block=False)
                else:
                    self.bad += 1
                    CommonQueue.SysCQ.put({'Error': False, 'Event': 'Expected value != Current value', 'Object': str(self)}, block=False)
            elif rsp['value'] == 3:
                CommonQueue.SysCQ.put({'Error': True, 'Event': 'Test Failed. Pipe disconnected', 'Object': str(self)})
            else:
                CommonQueue.SysCQ.put({'Error': True, 'Event': 'Test Failed. Slider in unknown position', 'Object': str(self)}, block=False)
        else:
            log.info('{}: Not Received __node_sc_data__{}Waiting __node_sc_heartbeat__'.format(str(self), ' ' * 10))
            rsp = Worker.__wait_mqtt_rsp(cls=self.__object,
                                         keys=['value', 'sequence'],
                                         timeout_msec=70000,
                                         sub_command='node_sc_heartbeat')

            if rsp:
                if rsp['value'] == 1 or rsp['value'] == 2:
                    if rsp['value'] == self.__command['value']:
                        self.__command['value'] = 2 if rsp['value'] == 1 else 1
                        self.lost_node_sc_data += 1
                        self.bad += 1
                        CommonQueue.SysCQ.put({'Error': False, 'Event': 'Lost __node_sc_data__ from Device', 'Object': str(self)}, block=False)
                    else:
                        self.lost_command += 1
                        self.bad += 1
                        CommonQueue.SysCQ.put({'Error': False, 'Event': 'Device lost __command__ from Tablet', 'Object': str(self)}, block=False)
                elif rsp['value'] == 3:
                    CommonQueue.SysCQ.put({'Error': True, 'Event': 'Test Failed. Pipe disconnected', 'Object': str(self)}, block=False)
                else:
                    CommonQueue.SysCQ.put({'Error': True, 'Event': 'Test Failed. Slider in unknown position', 'Object': str(self)}, block=False)
            else:
                CommonQueue.SysCQ.put({'Error': True, 'Event': 'Test Failed. Lost device', 'Object': str(self)}, block=False)

    def run(self):
        self.lost_command = 0
        self.common_count = 0
        self.lost_node_sc_data = 0
        self.good = 0
        self.bad = 0
        self.wait = True
        self.break_test = False

        self.__set_up()

        for cycle in range(self.__cycles):
            while self.wait:
                sleep(0.02)
            if self.break_test:
                break
            self.wait = True
            self.common_count += 1
            self.__send_command()

        log.info('{}: TEST STOP'.format(str(self)))
        CommonQueue.SysCQ.put({'Error': True, 'Event': 'Test was Ended', 'Object': str(self)}, block=False)

    @staticmethod
    def __wait_mqtt_rsp(cls, keys, timeout_msec, sub_command=None):
        rsp = collections.defaultdict(str)

        remaining_time = timeout_msec / 1000
        time_end = time.time() + remaining_time

        while remaining_time > 0:
            try:
                msg = cls.get_rx_msg(remaining_time)
                if msg is not None:
                    d_payload = json.loads(msg)

                    if sub_command:
                        if sub_command in d_payload.values():

                            for key in keys:
                                if key in d_payload.keys():
                                    rsp[key] = d_payload[key]

                            if len(rsp) == len(keys):
                                break

            except KeyboardInterrupt:
                raise
            except:
                pass
            remaining_time = time_end - time.time()
        return rsp


if __name__ == '__main__':
    worker = Worker(ip='10.8.0.15', sn='00124b001d035fc2', cycles=50)
    worker.start()

    while True:
        while CommonQueue.SysCQ.empty():
            sleep(0.01)

        try:
            data = CommonQueue.SysCQ.get(block=False)
        except Exception as err:
            log.error(err)
        else:
            if data['Error']:
                worker.wait = False
                worker.break_test = True
            else:
                worker.wait = False

            log.info('')
            log.info('{}:'.format(worker))
            log.info('Common number: ------------------------------- {}'.format(worker.common_count))
            log.info('   |-> Good: --------------------------------- {}'.format(worker.good))
            log.info('   |-> Bad: ---------------------------------- {}'.format(worker.bad))
            log.info('        |-> Lost __node_sc_data__ from device: {}'.format(worker.lost_node_sc_data))
            log.info('        |-> Lost __command__ from Tablet: ---- {}'.format(worker.lost_command))



