# -*- coding: utf-8 -*-

import json

from aliyun.log.consumer.consumer_group_request import ConsumerGroupRequest


class ConsumerGroupHeartBeatRequest(ConsumerGroupRequest):
    def __init__(self, project, logstore, consumer_group, consumer, shards):
        ConsumerGroupRequest.__init__(self, project, logstore)
        self.consumer_group = consumer_group
        self.consumer = consumer
        self.shards = shards

    def get_params(self):
        params = {
            'type': 'heartbeat',
            'consumer': self.consumer
        }
        return params

    def get_shards(self):
        return self.shards

    def set_shards(self, shards):
        self.shards = shards

    def get_request_body(self):
        return json.dumps(self.shards)
