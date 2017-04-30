# -*- coding: utf-8 -*-

from aliyun.log.logresponse import LogResponse


class ConsumerGroupUpdateCheckPointResponse(LogResponse):

    def __init__(self, headers):
        LogResponse.__init__(self, headers)
