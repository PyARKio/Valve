# -- coding: utf-8 --
from __future__ import unicode_literals
from time import sleep
from Utilits.log_settings import log
from Utilits.CommonQueue import CommonQueue
from Utilits.worker import Worker
import configparser
import platform
try:
    import curses
except:
    pass
from time import sleep
from random import randint


config = configparser.ConfigParser()
config.read('./valves.config')
valves = config['Devices']['Valves'].strip('][').split(', ')
cycles = int(config['Devices']['Cycles'])
delay_list_int = [int(delay)*1000 for delay in config['Devices']['Delay'].strip('][').split(', ')]
wait_HB_after_comm = int(config['Devices']['Wait_HB_after_comm'])
ip = config['Connection']['IP']


def linux_distribution():
    try:
        return platform.linux_distribution()
    except:
        return "N/A"


def get_done(workers=None):
    for value in list(workers.values()):
        if value['State'] == 'Waiting':
            return False
    return True


def send_all(workers=None):
    for key, value in workers.items():
        value['State'] = 'Waiting'
        value['Worker'].wait = False


def logout(workers=None):
    global cycles

    if get_done(workers=workers):
        for device, worker in workers.items():
            log.info('')
            log.info('{}:'.format(device))
            log.info(
                '{}: Common number: ------------------------------- {}/{}'.format(device, worker['Worker'].common_count, cycles))
            log.info('{}:    |-> Good: --------------------------------- {}'.format(device, worker['Worker'].good))
            log.info('{}:    |-> Bad: ---------------------------------- {}'.format(device, worker['Worker'].bad))
            log.info('{}:         |-> Lost __node_sc_data__ from device: {}'.format(device, worker['Worker'].lost_node_sc_data))
            log.info('{}:         |-> Lost __command__ from Tablet: ---- {}'.format(device, worker['Worker'].lost_command))

        sleep(randint(delay_list_int[0], delay_list_int[1]))
        send_all(workers=workers)


def run():
    global valves
    global cycles
    global ip

    workers = dict()
    for valve in valves:
        workers[valve] = {'Worker': Worker(ip=ip, sn=valve, cycles=cycles+1, wait_hb_after_comm=wait_HB_after_comm), 'State': 'Waiting'}
        workers[valve]['Worker'].start()

    flag = True
    while flag:
        while CommonQueue.SysCQ.empty():
            sleep(0.01)

        try:
            data = CommonQueue.SysCQ.get(block=False)
        except Exception as err:
            log.error(err)
        else:
            log.info(data)
            if data['Error']:
                log.info(workers)
                if workers.get(data['Object']):
                    workers[data['Object']]['Worker'].wait = False
                    workers[data['Object']]['Worker'].break_test = True
                    log.info(workers[data['Object']]['Worker'].is_alive())
                    while workers[data['Object']]['Worker'].is_alive():
                        sleep(0.07)
                    del workers[data['Object']]
                    log.info(workers)

                    if data['Event'] == 'Test was Ended':
                        break

                    logout(workers=workers)
            else:
                workers[data['Object']]['State'] = 'Done'
                logout(workers=workers)


if __name__ == '__main__':
    # if linux_distribution():
    run()
    # else:
    #     log.error('')


