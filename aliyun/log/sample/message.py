# -*- coding:utf-8 -*-


class Message(object):
    def __init__(self, value, time, key=None):
        """
        :type key: str
        :type value: str
        :type time: int
        """
        self.key = key
        self.value = value
        self.time = time
