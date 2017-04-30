# -×- coding: utf-8 -*-

from aliyun.log.logresponse import LogResponse


class CreateConsumerGroupResponce(LogResponse):

    def __init__(self, headers):
        LogResponse.__init__(self, headers)
        # super(CreateConsumerGroupResponce, self).__init__(headers)
