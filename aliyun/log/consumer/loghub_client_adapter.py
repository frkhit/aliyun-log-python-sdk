# -×- coding: utf-8 -*-

import rwlock
import logging

from aliyun.log.logexception import LogException

from aliyun.log.consumer.consumer_group_client import LogConsumerClient
from aliyun.log.consumer.loghub_exceptions.loghub_check_point_exception import LogHubCheckPointException


class LogHubClientAdapter(object):

    def __init__(self, endpoint, accessKeyId, accessKey, project,
                 logstore, consumer_group, consumer, securityToken=None):
        self.mclient = LogConsumerClient(endpoint, accessKeyId, accessKey, securityToken)
        self.mproject =project
        self.mlogstore = logstore
        self.mconsumer_group = consumer_group
        self.mconsumer = consumer
        self.rw_lock = rwlock.RWLock()
        self.logger = logging.getLogger(self.__class__.__name__)

    def swith_client(self, endpoint, accessKeyId, accessKey, securityToken=None):
        self.rw_lock.writer_lock.acquire()
        self.mclient = LogConsumerClient(endpoint, accessKeyId, accessKey, securityToken)
        self.rw_lock.writer_lock.release()

    def create_consumer_grouop(self, timeout, in_order):
        self.rw_lock.reader_lock.acquire()
        try:
            self.mclient.create_consumer_group(self.mproject, self.mlogstore, self.mconsumer_group, timeout, in_order)
        finally:
            self.rw_lock.reader_lock.release()

    def get_consumer_group(self):
        self.rw_lock.reader_lock.acquire()
        try:
            for consumer_group in self.mclient.list_consumer_group(self.mproject, self.mlogstore).get_consumer_groups():
                if consumer_group.get_consumer_group_name() == self.mconsumer_group:
                    return consumer_group
        finally:
            self.rw_lock.reader_lock.release()
        return None

    def heartbeat(self, shards, responce=None):
        if responce is None:
            responce = []
        self.rw_lock.reader_lock.acquire()
        try:
            responce.extend(
                self.mclient.heart_beat(self.mproject, self.mlogstore, self.mconsumer_group, self.mconsumer, shards).get_shards())
            return True
        except LogException, e:
            self.logger.warn(e)
        finally:
            self.rw_lock.reader_lock.release()
        return False

    def update_check_point(self, shard, consumer, check_point):
        self.rw_lock.reader_lock.acquire()
        try:
            self.mclient.update_check_point(self.mproject, self.mlogstore, self.mconsumer_group, shard, check_point, consumer)
        finally:
            self.rw_lock.reader_lock.release()

    def get_check_point(self, shard):
        self.rw_lock.reader_lock.acquire()
        try:
            check_points = self.mclient.get_check_point(self.mproject, self.mlogstore, self.mconsumer_group, shard)\
                .get_consumer_group_check_points()
        finally:
            self.rw_lock.reader_lock.release()
        if check_points is None or len(check_points) == 0:
            raise LogHubCheckPointException('fail to get shard check point')
        else:
            return check_points[0]

    def get_cursor(self, shard_id, start_time):
        self.rw_lock.reader_lock.acquire()
        try:
            return self.mclient.get_cursor(self.mproject, self.mlogstore, shard_id, start_time).get_cursor()
        finally:
            self.rw_lock.reader_lock.release()

    def get_begin_cursor(self, shard_id):
        self.rw_lock.reader_lock.acquire()
        try:
            return self.mclient.get_begin_cursor(self.mproject, self.mlogstore, shard_id).get_cursor()
        finally:
            self.rw_lock.reader_lock.release()

    def get_end_cursor(self, shard_id):
        self.rw_lock.reader_lock.acquire()
        try:
            return self.mclient.get_end_cursor(self.mproject, self.mlogstore, shard_id).get_cursor()
        finally:
            self.rw_lock.reader_lock.release()

    def pull_logs(self, shard_id, cursor, count=1000):
        self.rw_lock.reader_lock.acquire()
        try:
            return self.mclient.pull_logs(self.mproject, self.mlogstore, shard_id, cursor, count)
        finally:
            self.rw_lock.reader_lock.release()
