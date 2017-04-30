# -*- coding: utf-8 -*-

import time

from aliyun.log.consumer.loghub_task import LoghubProcessorBase, LoghubProcessorFactory
from aliyun.log.consumer.client_worker import ClientWorker
from aliyun.log.consumer.config import LoghubConfig
from aliyun.log.consumer.config import LoghubCursorPosition
from aliyun.log.consumer.consumer_group_client import LogConsumerClient



class SampleConsuemr(LoghubProcessorBase):

    shard_id = -1
    last_check_time = 0

    def initialize(self, shard):
        self.shard_id = shard

    def process(self, log_groups, check_point_tracker):
        for log_group in log_groups.LogGroups:
            items = []
            for log in log_group.Logs:
                item = dict()
                item['time'] = log.Time
                for content in log.Contents:
                    item[content.Key] = content.Value
                items.append(item)
            log_items = dict()
            log_items['topic'] = log_group.Topic
            log_items['source'] = log_group.Source
            log_items['logs'] = items
            print log_items
        current_time = time.time()
        if current_time - self.last_check_time > 3:
            try:
                check_point_tracker.save_check_point(True)
            except:
                import traceback
                traceback.print_exc()
        else:
            try:
                check_point_tracker.save_check_point(False)
            except:
                import traceback
                traceback.print_exc()
        # 返回空表示正常处理，需要返回上一个checkpoint，return check_point_tracker.get_check_point()
        return None

    def shutdown(self, check_point_tracker):
        try:
            check_point_tracker.save_check_point(True)
        except Exception, e:
            import traceback
            print traceback.print_exc()


class SampleLoghubFactory(LoghubProcessorFactory):

    def generate_processor(self):
        return SampleConsuemr()


endpoint = ''  # 选择与上面步骤创建Project所属区域匹配的Endpoint
accessKeyId = ''         # 使用你的阿里云访问秘钥AccessKeyId
accessKey = ''              # 使用你的阿里云访问秘钥AccessKeySecret
project = 'score-rule-test'                  # 上面步骤创建的项目名称
logstore = 'score-test1'                 # 上面步骤创建的日志库名称
client = LogConsumerClient(endpoint, accessKeyId, accessKey)


loghub_config = LoghubConfig(endpoint, accessKeyId, accessKey, project, logstore, 'consumergroup',
                             'consumer_1', cursor_position=LoghubCursorPosition.BEGIN_CURSOR, heartbeat_interval=20,
                             data_fetch_interval=1)


def main():
    sample_loghub_factory = SampleLoghubFactory()
    client_worker = ClientWorker(sample_loghub_factory, loghub_config=loghub_config)
    client_worker.start()


if __name__ == '__main__':
    main()
