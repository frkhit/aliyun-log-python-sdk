# -*- coding: utf-8 -*-

import logging
import time

from aliyun.log.consumer.loghub_exceptions.loghub_check_point_exception import LogHubCheckPointException
from aliyun.log.logexception import LogException

logger = logging.getLogger(__name__)


class LoghubCheckpointTracker(object):
    def __init__(self, loghub_client_adapter, consumer_name, shard_id):
        self.loghub_client_adapter = loghub_client_adapter
        self.consumer_name = consumer_name
        self.shard_id = shard_id
        self.last_check_time = time.time()
        self.cursor = ''
        self.temp_check_point = ''
        self.last_persistent_checkpoint = ''
        self.default_flush_check_point_interval = 60

    def set_cursor(self, cursor):
        self.cursor = cursor

    def get_cursor(self):
        return self.cursor

    def save_check_point(self, persistent, cursor=None):
        if cursor is not None:
            self.temp_check_point = cursor
        else:
            self.temp_check_point = self.cursor
        if persistent:
            self.flush_check_point()

    def set_memory_check_point(self, cursor):
        self.temp_check_point = cursor

    def set_persistent_check_point(self, cursor):
        self.last_persistent_checkpoint = cursor

    def flush_check_point(self):
        if self.temp_check_point != '' and self.temp_check_point != self.last_persistent_checkpoint:
            try:
                self.loghub_client_adapter.update_check_point(self.shard_id, self.consumer_name, self.temp_check_point)
                self.last_persistent_checkpoint = self.temp_check_point
            except LogException, e:
                raise LogHubCheckPointException("Failed to persistent the cursor to outside system, "
                                                + self.consumer_name + ", "
                                                + str(self.shard_id) + ", "
                                                + self.temp_check_point,
                                                e)

    def flush_check(self):
        current_time = time.time()
        if current_time > self.last_check_time + self.default_flush_check_point_interval:
            try:
                self.flush_check_point()
            except LogHubCheckPointException, e:
                logger.error(e)
            self.last_check_time = current_time

    def get_check_point(self):
        return self.temp_check_point
