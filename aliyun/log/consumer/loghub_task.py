# -*- coding: utf-8 -*-

import logging

from aliyun.log.consumer.config import LoghubCursorPosition
from aliyun.log.logexception import LogException

logger = logging.getLogger(__name__)


class LoghubProcessorBase(object):
    def initialize(self, shard):
        raise NotImplementedError('not initialize shard')

    def process(self, log_groups, check_point_tracker):
        raise NotImplementedError('not create method process')

    def shutdown(self, check_point_tracker):
        raise NotImplementedError('not create method shutdown')


class LoghubProcessorFactory(object):
    def generate_processor(self):
        raise NotImplementedError('not implement method generate_process')


class TaskResult(object):
    def __init__(self, task_exception):
        self.task_exception = task_exception

    def get_exception(self):
        return self.task_exception


class ProcessTaskResult(TaskResult):
    def __init__(self, rollback_check_point):
        super(ProcessTaskResult, self).__init__(None)
        self.rollback_check_point = rollback_check_point

    def get_rollback_check_point(self):
        return self.rollback_check_point


class InitTaskResult(TaskResult):
    def __init__(self, cursor, cursor_persistent):
        super(InitTaskResult, self).__init__(None)
        self.cursor = cursor
        self.cursor_persistent = cursor_persistent

    def get_cursor(self):
        return self.cursor

    def is_cursor_persistent(self):
        return self.cursor_persistent


class FetchTaskResult(TaskResult):
    def __init__(self, fetched_log_group_list, cursor):
        super(FetchTaskResult, self).__init__(None)
        self.fetched_log_group_list = fetched_log_group_list
        self.cursor = cursor

    def get_fetched_log_group_list(self):
        return self.fetched_log_group_list

    def get_cursor(self):
        return self.cursor


# task运行失败则返回TaskResult，否则返回对应的结果
# 运行任务， 该任务在concurrent.futures的ThreadPoolExecutor中submit
def process_task(processor, log_groups, check_point_tracker):
    try:
        check_point = processor.process(log_groups, check_point_tracker)
        check_point_tracker.flush_check()
    except Exception, e:
        import traceback
        traceback.print_exc()
        return TaskResult(e)
    return ProcessTaskResult(check_point)


# task运行失败则返回TaskResult，否则返回对应的结果
# 初始化任务，该任务在concurrent.futures的ThreadPoolExecutor中submit
def initialize_task(processor, loghub_client_adapter, shard_id, cursor_position, cursor_start_time):
    try:
        processor.initialize(shard_id)
        is_cursor_persistent = False
        check_point = loghub_client_adapter.get_check_point(shard_id)
        if check_point[u'checkpoint'] and len(check_point[u'checkpoint']) > 0:
            is_cursor_persistent = True
            cursor = check_point[u'checkpoint']
        else:
            if cursor_position == LoghubCursorPosition.BEGIN_CURSOR:
                cursor = loghub_client_adapter.get_begin_cursor(shard_id)
            elif cursor_position == LoghubCursorPosition.END_CURSOR:
                cursor = loghub_client_adapter.get_end_cursor(shard_id)
            else:
                cursor = loghub_client_adapter.get_cursor(shard_id, cursor_start_time)
        return InitTaskResult(cursor, is_cursor_persistent)
    except Exception, e:
        return TaskResult(e)


# 这里每次抓取的loggroup数量写的是固定数，
def loghub_fetch_task(loghub_client_adapter, shard_id, cursor):
    max_fetch_log_group_size = 1000
    exception = None

    for retry_times in range(3):
        try:
            response = loghub_client_adapter.pull_logs(shard_id, cursor, count=max_fetch_log_group_size)
            fetch_log_group_list = response.get_loggroup_list()
            logger.debug("shard id = " + str(shard_id) + " cursor = " + cursor
                         + " next cursor" + response.get_next_cursor() + " size:" + str(response.get_log_count()))
            next_cursor = response.get_next_cursor()
            if not next_cursor:
                return FetchTaskResult(fetch_log_group_list, cursor)
            else:
                return FetchTaskResult(fetch_log_group_list, next_cursor)
        except LogException, e:
            exception = e
        except Exception, e1:
            logger.error(e1, exc_info=True)
            raise Exception(e1)

        # only retry if the first request get "SLSInvalidCursor" exception
        if retry_times == 0 and isinstance(exception, LogException) \
                and 'invalidcursor' in exception.get_error_code().lower():
            try:
                cursor = loghub_client_adapter.get_end_cursor(shard_id)
            except Exception, e:
                return TaskResult(exception)
        else:
            break

    return TaskResult(exception)


# task运行失败则返回TaskResult，成功则停止任务无返回
# shutdown task
def shutdown_task(processor, check_point_tracker):
    exception = None
    try:
        processor.shutdown(check_point_tracker)
    except Exception, e:
        print e
        exception = None

    try:
        check_point_tracker.get_check_point()
    except Exception, e:
        logger.error('Failed to flush check point', exc_info=True)

    return TaskResult(exception)
