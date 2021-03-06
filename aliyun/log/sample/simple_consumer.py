# -*- coding:utf-8 -*-
import logging
import time
from Queue import Queue

from aliyun.log.consumer.client_worker import ClientWorker
from aliyun.log.consumer.config import LoghubConfig, LoghubCursorPosition
from aliyun.log.consumer.loghub_checkpoint_tracker import LoghubCheckpointTracker
from aliyun.log.consumer.loghub_task import LoghubProcessorBase, LoghubProcessorFactory
from aliyun.log.sample.simple_utils import create_guid, json2pyobj, get_log_key
from message import Message

logger = logging.getLogger(__name__)


class SampleConsumer(LoghubProcessorBase):
    shard_id = -1
    last_check_time = 0
    _log_key = get_log_key()

    def __init__(self, log_queue):
        """
        :type log_queue: Queue
        """
        super(SampleConsumer, self).__init__()
        assert log_queue is not None
        self._log_queue = log_queue

    def initialize(self, shard):
        self.shard_id = shard

    def process(self, log_groups, check_point_tracker):
        """
        :type log_groups:
        :type check_point_tracker: LoghubCheckpointTracker
        """
        for log_group in log_groups.LogGroups:
            for log in log_group.Logs:
                for content in log.Contents:
                    if content.Key == self._log_key:
                        dict_log = json2pyobj(content.Value)
                        msg = Message(value=dict_log.get("v"), key=dict_log.get('k'), time=log.Time)
                        self._log_queue.put(msg)

        current_time = time.time()
        if current_time - self.last_check_time > 3:
            try:
                check_point_tracker.save_check_point(True)
            except Exception as e:
                logger.error(e, exc_info=True)
        else:
            try:
                check_point_tracker.save_check_point(False)
            except Exception as e:
                logger.error(e, exc_info=True)
        # 返回空表示正常处理，需要返回上一个checkpoint，return check_point_tracker.get_check_point()
        return None

    def shutdown(self, check_point_tracker):
        try:
            check_point_tracker.save_check_point(True)
        except Exception as e:
            logger.error(e, exc_info=True)


class SampleLoghubFactory(LoghubProcessorFactory):
    def __init__(self, log_queue):
        super(SampleLoghubFactory, self).__init__()
        self._log_queue = log_queue

    def generate_processor(self):
        return SampleConsumer(self._log_queue)


class SimpleCorLoghubConsumer(object):
    def __init__(self, logstore, project, endpoint, access_key_id, access_key, group="sss1",
                 consumer_name=create_guid(), heartbeat_interval_in_second=10, data_fetch_interval_in_second=1,
                 cursor_position=LoghubCursorPosition.BEGIN_CURSOR, log_queue=None,
                 can_update_consumer_group=False):

        loghub_config = LoghubConfig(endpoint, access_key_id,
                                     access_key, project, logstore, group,
                                     consumer_name, cursor_position=cursor_position,
                                     heartbeat_interval=heartbeat_interval_in_second,
                                     data_fetch_interval=data_fetch_interval_in_second,
                                     can_update_consumer_group=can_update_consumer_group)

        self.log_queue = log_queue or Queue(10)
        sample_loghub_factory = SampleLoghubFactory(self.log_queue)
        client_worker = ClientWorker(sample_loghub_factory, loghub_config=loghub_config)
        client_worker.setDaemon(True)
        client_worker.start()

        self._iterator = self._message_generator()
        self._closed = False

    def __iter__(self):
        return self

    def next(self):
        if self._closed:
            raise Exception("consumer is closed!")

        try:
            return next(self._iterator)
        except StopIteration:
            raise

    def close(self):
        self._iterator = None
        self._closed = True

    def _message_generator(self):
        while True:
            log = self.log_queue.get()
            if log is not None and isinstance(log, Message):
                yield log
