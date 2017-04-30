# -×- coding: utf-8 -*-

import logging
import time

from threading import Thread

logger = logging.getLogger(__name__)


class LoghubHeartBeat(Thread):
    def __init__(self, loghub_client_adapter, heartbeat_interval):
        super(LoghubHeartBeat, self).__init__()
        self.loghub_client_adapter = loghub_client_adapter
        self.heartbeat_interval = heartbeat_interval
        self.held_shards = []
        self.heart_shards = []
        self.shut_down_flag = False

    def run(self):
        logging.debug('heart beat start')
        while not self.shut_down_flag:
            try:
                shards = self.heart_shards[:]
                held_shards = []
                # 此处要注意，务必使得held_shards = []，从而在函数中改变held_shards
                self.loghub_client_adapter.heartbeat(shards, held_shards)
                self.held_shards = held_shards
                self.heart_shards = self.held_shards[:]
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(e, exc_info=1)

    def get_held_shards(self):
        return self.held_shards[:]

    def shutdown(self):
        logger.debug('heart beat stop')
        self.shut_down_flag = True

    def remove_heart_shard(self, shard):
        if shard in self.held_shards:
            self.heart_shards.remove(shard)
