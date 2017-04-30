# -*- coding: utf-8 -*-

from aliyun.log.logresponse import LogResponse

from aliyun.log.consumer.consumer_group import ConsumerGroup


class ListConsumerGroupResponse(LogResponse):

    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers)
        self.count = len(resp)
        self.resp = resp
        self.consumer_groups =[ConsumerGroup(group[u'name'], group[u'timeout'], group[u'order']) for group in self.resp]

    def get_count(self):
        return self.count

    def get_consumer_groups(self):
        return self.consumer_groups

    def log_print(self):
        print 'ListConsumerGroupResponse:'
        print 'headers:', self.get_all_headers()
        print 'count:', self.count
        print 'consumer_groups:', self.resp
