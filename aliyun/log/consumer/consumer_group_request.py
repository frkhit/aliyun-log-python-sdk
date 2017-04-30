# -*- coding: utf-8 -*-

from aliyun.log.logrequest import LogRequest


class ConsumerGroupRequest(LogRequest):
    def __init__(self, project, logstore):
        LogRequest.__init__(self, project)
        # super(ConsumerGroupRequest, self).__init__(project)
        self.logstore = logstore

    def get_logstore(self):
        return self.logstore

    def set_logstore(self, logstore):
        self.logstore = logstore
