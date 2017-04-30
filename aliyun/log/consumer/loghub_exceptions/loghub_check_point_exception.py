# -*- coding: utf-8 -*-


class LogHubCheckPointException(Exception):

    def __init__(self, message, cause=None):
        if cause is not None:
            super(LogHubCheckPointException, self).__init__(message, cause)
        else:
            super(LogHubCheckPointException, self).__init__(message)
