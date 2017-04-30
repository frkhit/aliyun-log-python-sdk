# -*- coding: utf-8 -*-

from aliyun.log.consumer.consumer_group_request import ConsumerGroupRequest


class CreateConsumerGroupRequest(ConsumerGroupRequest):

    def __init__(self, project, logstore, consumer_group):
        ConsumerGroupRequest.__init__(self, project, logstore)
        # super(CreateConsumerGroupRequest, self).__init__(project, logstore)
        self.consumer_group = consumer_group

    def get_consumer_group(self):
        return self.consumer_group

    def set_consuemr_group(self, consumer_group):
        self.consumer_group = consumer_group
