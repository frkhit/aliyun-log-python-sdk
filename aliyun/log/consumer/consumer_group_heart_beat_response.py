# -*- coding: utf-8 -*-

from aliyun.log.logresponse import LogResponse


class ConsumerGroupHeartBeatResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers)
        self.shards = resp

    def get_shards(self):
        return self.shards

    def set_shards(self, shards):
        self.shards = shards

    def log_print(self):
        print 'ListHeartBeat:'
        print 'headers:', self.get_all_headers()
        print 'shards:', self.shards
