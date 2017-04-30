# -×- coding: utf-8 -*-

import logging
import time

from threading import Thread

logger = logging.getLogger(__name__)


class LoghubHeartBeat(Thread):

    def __init__(self, loghub_client_adapter, heartbeat_interval):
        super(LoghubHeartBeat, self).__init__()
        self.mloghub_client_adapter = loghub_client_adapter
        self.heartbeat_interval = heartbeat_interval
        self.mheld_shards = []
        self.mheart_shards = []
        self.shut_down_flag = False

    def run(self):
        logging.debug('heart beat start')
        while not self.shut_down_flag:
            try:
                shards = self.mheart_shards[:]
                mheld_shards = []
                # 此处要注意，务必使得mheld_shards = []，从而在函数中改变mheld_shards
                self.mloghub_client_adapter.heartbeat(shards, mheld_shards)
                self.mheld_shards = mheld_shards
                self.mheart_shards = self.mheld_shards[:]
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                print e

    def get_held_shards(self):
        return self.mheld_shards[:]

    def shutdown(self):
        logging.debug('heart beat stop')
        self.shut_down_flag = True

    def remove_heart_shard(self, shard):
        if shard in self.mheld_shards:
            self.mheart_shards.remove(shard)
