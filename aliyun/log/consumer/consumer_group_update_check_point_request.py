# -*- coding: utf-8 -*-

import json

from aliyun.log.consumer.consumer_group_request import ConsumerGroupRequest


class ConsumerGroupUpdateCheckPointRequest(ConsumerGroupRequest):

    def __init__(self, project, logstore, consumer_group, consumer, shard, check_point, force_success=True):
        ConsumerGroupRequest.__init__(self, project, logstore)
        self.consumer_group = consumer_group
        self.consumer = consumer
        self.shard = shard
        self.check_point = check_point
        self.force_success = force_success

    def get_consumer_group(self):
        return self.consumer_group

    def set_consumer_group(self, consumer_group):
        self.consumer_group = consumer_group

    def get_request_body(self):
        body_dict = {
            'shard': int(self.shard),
            'checkpoint': self.check_point
        }
        return json.dumps(body_dict)

    def get_request_params(self):
        params = {
            'type': 'checkpoint',
            'consumer': self.consumer,
            'forceSuccess': self.force_success
        }
        return params
