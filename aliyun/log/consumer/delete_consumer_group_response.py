# -*- coding: utf-8 -*-

from aliyun.log.logresponse import LogResponse


class DeleteConsumerGroupResponse(LogResponse):
    def __init__(self, headers):
        LogResponse.__init__(self, headers)
