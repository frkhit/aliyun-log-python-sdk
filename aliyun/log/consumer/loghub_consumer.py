# -*- coding: utf-8 -*-

import logging
import time
import copy

from concurrent.futures import ThreadPoolExecutor

from aliyun.log.consumer.config import ConsumerStatus, LoghubCursorPosition
from aliyun.log.consumer.loghub_client_adapter import LogHubClientAdapter
from aliyun.log.consumer.loghub_checkpoint_tracker import LoghubCheckpointTracker
from aliyun.log.consumer.fetched_log_group import FetchedLogGroup
from aliyun.log.consumer.loghub_task import ProcessTaskResult, TaskResult, InitTaskResult
from aliyun.log.consumer.loghub_task import loghub_fetch_task, initialize_task, process_task, shutdown_task


class LoghubConsuemr(object):

    def __init__(self, loghub_client_adapter, shard_id, consumer_name, processor, cursor_position, cursor_start_time,
                 max_workers=2):
        self.loghub_client_adapter = loghub_client_adapter
        self.shard_id = shard_id
        self.consumer_name = consumer_name
        self.cursor_position = cursor_position
        self.cursor_start_time = cursor_start_time
        self.processor = processor
        self.check_point_tracker = LoghubCheckpointTracker(self.loghub_client_adapter, self.consumer_name,
                                                           self.shard_id)
        self.excutor = ThreadPoolExecutor(max_workers=max_workers)
        self.consumer_status = ConsumerStatus.INITIALIZING
        self.current_task_exist = False
        self.task_future = None
        # self.task_future = self.excutor.submit(add, 1, 2)
        self.fetch_data_future = None
        # self.fetch_data_future = self.excutor.submit()

        self.next_fetch_cursor = ''
        self.shutdown = False
        self.last_fetch_log_group = None

        self.last_log_error_time = 0
        self.last_fetch_time = 0
        self.last_fetch_count = 0
        # 在python pull logs responce 中未获取
        # self.last_fetch_raw_size = 0

        self.logger = logging.getLogger(__name__)

    def consume(self):
        logging.debug('consumer start consuming')
        self.check_and_generate_next_task()
        if self.consumer_status == ConsumerStatus.PROCESSING and self.last_fetch_log_group is None:
            self.fetch_data()

    @staticmethod
    # get future (if failed return None)
    def get_task_result(task_future):
        if task_future is not None and task_future.done():
            try:
                return task_future.result()
            except Exception, e:
                import traceback
                traceback.print_exc()
        return None

    def fetch_data(self):
        # 无抓取任务或抓取结束
        if self.fetch_data_future is None or self.fetch_data_future.done():
            task_result = self.get_task_result(self.fetch_data_future)
            # 抓取成功，输出结果，同时获取next_cursor
            if task_result is not None and task_result.get_exception() is None:
                self.last_fetch_log_group = FetchedLogGroup(self.shard_id, task_result.get_fetched_log_group_list(),
                                                            task_result.get_cursor())
                self.next_fetch_cursor = task_result.get_cursor()
                self.last_fetch_count = self.last_fetch_log_group.get_log_group_size()
                # self.last_fetch_raw_size = task_result.get_raw_size()

            self.sample_log_error(task_result)
            # 无抓取任务或抓取成功， 则生成新的抓取任务
            if task_result is None or task_result.get_exception() is None:
                # 判断是否生成抓取任务
                is_generate_fetch_task = True
                # 拥塞控制 参照java部分实现
                if self.last_fetch_count < 100:
                    is_generate_fetch_task = (time.time() - self.last_fetch_time) > 0.5
                elif self.last_fetch_count < 500:
                    is_generate_fetch_task = (time.time() - self.last_fetch_time) > 0.2
                elif self.last_fetch_count < 1000:
                    is_generate_fetch_task = (time.time() - self.last_fetch_time) > 0.05

                if is_generate_fetch_task:
                    self.last_fetch_time = time.time()
                    self.fetch_data_future = \
                        self.excutor.submit(loghub_fetch_task,
                                            self.loghub_client_adapter, self.shard_id, self.next_fetch_cursor)
                else:
                    self.fetch_data_future = None
            else:
                self.fetch_data_future = None

    def check_and_generate_next_task(self):

        if self.task_future is None or self.task_future.done():
            task_success = False
            # get the task_future result: 三种情况，未完成或失败不处理，
            # 成功返回TaskResult或者ProcessTaskResult、InitTaskResult的实例
            task_result = self.get_task_result(self.task_future)
            self.task_future = None

            if task_result is not None and task_result.get_exception() is None:

                task_success = True
                if self.consumer_status == ConsumerStatus.INITIALIZING:
                    # task_future返回的类型是InitTaskResult的实例
                    init_result = task_result
                    self.next_fetch_cursor = init_result.get_cursor()
                    self.check_point_tracker.set_memory_check_point(self.next_fetch_cursor)
                    if init_result.is_cursor_persistent():
                        self.check_point_tracker.set_persistent_check_point(self.next_fetch_cursor)

                elif isinstance(task_result, ProcessTaskResult):
                    # task_future返回的类型是ProcessTaskResult的实例
                    process_task_result = task_result
                    roll_back_checkpoint = process_task_result.get_rollback_check_point()
                    if roll_back_checkpoint is not None and roll_back_checkpoint != '':
                        self.last_fetch_log_group = None
                        self.cancel_current_fetch()
                        self.next_fetch_cursor = roll_back_checkpoint

            # 任务发生exception写日志
            self.sample_log_error(task_result)
            self._update_status(task_success)
            self.generate_next_task()

    def generate_next_task(self):
        #
        if self.consumer_status == ConsumerStatus.INITIALIZING:
            self.current_task_exist = True
            self.task_future = self.excutor.submit(initialize_task, self.processor, self.loghub_client_adapter,
                                                   self.shard_id, self.cursor_position, self.cursor_start_time)

        elif self.consumer_status == ConsumerStatus.PROCESSING:
            if self.last_fetch_log_group is not None:
                self.check_point_tracker.set_cursor(self.last_fetch_log_group.get_end_cursor())
                self.current_task_exist = True
                # 在这里需要深拷贝，在submit之前修改 self.last_fetch_log_group，但同时需将修改前的值作为submit的参数
                last_fetch_log_group = copy.deepcopy(self.last_fetch_log_group)
                self.last_fetch_log_group = None
                self.task_future = self.excutor.submit(process_task, self.processor,
                                                       last_fetch_log_group.get_fetched_log_group_list(),
                                                       self.check_point_tracker)

        elif self.consumer_status == ConsumerStatus.SHUTTING_DOWN:
            self.current_task_exist = True
            self.cancel_current_fetch()
            self.task_future = self.excutor.submit(shutdown_task, self.processor, self.check_point_tracker)

    def cancel_current_fetch(self):
        if self.fetch_data_future is not None:
            self.fetch_data_future.cancel()
            self.logger.warning('Cancel a fetch task, shard id: ' + str(self.shard_id))
            self.fetch_data_future = None

    def sample_log_error(self, result):
        # 发生exception 记录
        current_time = time.time()
        if result is not None \
            and result.get_exception() is not None \
                and current_time - self.last_log_error_time > 5:
            self.logger.warning(result.get_exception(), exc_info=True)
            self.last_log_error_time = current_time

    def _update_status(self, task_succcess):

        if self.consumer_status == ConsumerStatus.SHUTTING_DOWN:
            if self.current_task_exist is False or task_succcess:
                self.consumer_status = ConsumerStatus.SHUTDOWN_COMPLETE

        elif self.shutdown:
            self.consumer_status = ConsumerStatus.SHUTTING_DOWN
        elif task_succcess:
            if self.consumer_status == ConsumerStatus.INITIALIZING:
                self.consumer_status = ConsumerStatus.PROCESSING

    def shut_down(self):
        self.shutdown = True
        if not self.is_shutdown():
            self.check_and_generate_next_task()

    def is_shutdown(self):
        return self.consumer_status == ConsumerStatus.SHUTDOWN_COMPLETE
