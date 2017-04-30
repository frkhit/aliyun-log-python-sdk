# -*- coding: utf-8 -*-

from aliyun.log.logresponse import LogResponse


class UpdateConsumerGroupResponse(LogResponse):
    def __init__(self, headers):
        LogResponse.__init__(self, headers)
