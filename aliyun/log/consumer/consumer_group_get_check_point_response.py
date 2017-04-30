# -*- coding: utf-8 -*-

from aliyun.log.logresponse import LogResponse


class ConsumerGroupCheckPointResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers)
        self.count = len(resp)
        self.consumer_group_check_points = resp

    def get_count(self):
        return self.count

    def get_consumer_group_check_points(self):
        return self.consumer_group_check_points

    def log_print(self):
        print 'ListConsumerGroupCheckPoints:'
        print 'headers:', self.get_all_headers()
        print 'count:', self.count
        print 'consumer_group_check_points:', self.consumer_group_check_points
