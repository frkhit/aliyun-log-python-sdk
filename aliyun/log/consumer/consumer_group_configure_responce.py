# -×- coding: utf-8 -*-

from aliyun.log.logresponse import LogResponse


class CreateConsumerGroupResponse(LogResponse):

    def __init__(self, headers):
        LogResponse.__init__(self, headers)
        # super(CreateConsumerGroupResponse, self).__init__(headers)
