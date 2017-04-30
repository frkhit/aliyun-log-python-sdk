# -*- coding: utf-8 -*-

from aliyun.log.consumer.consumer_group_request import ConsumerGroupRequest


class ConsumerGroupGetCheckPointRequest(ConsumerGroupRequest):

    def __init__(self, project, logstore, consumer_group, shard):
        ConsumerGroupRequest.__init__(self, project, logstore)
        self.consumer_group = consumer_group
        self.shard = shard

    def get_params(self):
        if self.shard >= 0:
            return {'shard': self.shard}
        else:
            return {}

    def get_consumer_group(self):
        return self.consumer_group

    def set_consumer_group(self, consumer_group):
        self.consumer_group = consumer_group


