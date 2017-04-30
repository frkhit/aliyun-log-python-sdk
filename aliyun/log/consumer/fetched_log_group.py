# -*- coding: utf-8 -×-

from aliyun.log.log_logs_pb2 import LogGroup
from aliyun.log.log_logs_pb2 import LogGroupList


class FetchedLogGroup(object):

    def __init__(self, shard_id, log_group_list, end_cursor):
        self._shard_id = shard_id
        self._fetched_log_group_list = log_group_list
        self._end_cursor = end_cursor

    def get_shard_id(self):
        return self._shard_id

    def get_fetched_log_group_list(self):
        return self._fetched_log_group_list

    def get_end_cursor(self):
        return self._end_cursor

    def get_log_group_size(self):
        return len(self._fetched_log_group_list.LogGroups)
