# -*- coding:utf-8 -*-
import abc
import logging
import time

from aliyun.log.logclient import LogClient
from aliyun.log.logexception import LogException
from aliyun.log.logitem import LogItem
from aliyun.log.putlogsrequest import PutLogsRequest
from aliyun.log.sample.simple_utils import pyobj2json, get_log_key

logger = logging.getLogger(__name__)


class BaseLoghubProducer(object):
    def __init__(self, project, endpoint, access_key_id, access_key, create_logstore_if_not_exists=False,
                 ttl_days_of_new_logstore=1, shard_count_of_new_logstore=2):
        self.client = LogClient(endpoint, access_key_id, access_key)
        self.project = project
        self.create_logstore_if_not_exists = create_logstore_if_not_exists
        self.ttl = ttl_days_of_new_logstore
        self.shard_count = shard_count_of_new_logstore

    @abc.abstractmethod
    def send(self, topic, value=None, key=None, source=None, logstore=None):
        """
        :type logstore: str
        :type source: str
        :type key: str
        :type value: str
        :type topic: str
        """
        pass

    @staticmethod
    def _value_to_string(key, value):
        dict_value = {'v': value}
        if key:
            dict_value['k'] = key
        return pyobj2json(dict_value)

    def _put_logs(self, topic, logstore, source, logitem_list):
        if self.create_logstore_if_not_exists:
            try:
                req = PutLogsRequest(self.project, logstore, topic, source, logitem_list)
                return self.client.put_logs(req)
            except LogException as e:
                logger.error("error code is %s" % e.get_error_code())
                if e.get_error_code() == "LogStoreNotExist":
                    # 新建logstore
                    self._create_logstore(logstore)
                    logger.info("create logstore %s" % logstore)
                    time.sleep(2)
                    req = PutLogsRequest(self.project, logstore, topic, source, logitem_list)
                    return self.client.put_logs(req)
        else:
            req = PutLogsRequest(self.project, logstore, topic, source, logitem_list)
            return self.client.put_logs(req)

    def _create_logstore(self, logstore):
        self.client.create_logstore(self.project, logstore_name=logstore, ttl=self.ttl, shard_count=self.shard_count)


class SimpleLoghubProducer(BaseLoghubProducer):
    def __init__(self, project, endpoint, access_key_id, access_key, create_logstore_if_not_exists=False,
                 ttl_days_of_new_logstore=1, shard_count_of_new_logstore=2):
        super(SimpleLoghubProducer, self).__init__(project, endpoint, access_key_id, access_key,
                                                   create_logstore_if_not_exists, ttl_days_of_new_logstore,
                                                   shard_count_of_new_logstore)
        self._key = get_log_key()

    def send(self, topic, value=None, key=None, source=None, logstore=None):
        # todo 方向：缓存日志，定时上传
        log_item = LogItem()
        log_item.set_time(int(time.time()))
        log_item.set_contents([(self._key, BaseLoghubProducer._value_to_string(key, value))])
        if logstore is None:
            loginfo = (topic, topic, source, [log_item])
        else:
            loginfo = (topic, logstore, source, [log_item])

        self._put_logs(*loginfo)
