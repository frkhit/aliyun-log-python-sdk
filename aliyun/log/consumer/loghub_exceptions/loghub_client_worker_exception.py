# -*- coding: utf-8 -*-


class LogHubClientWorkerException(Exception):

    def __init__(self, message, cause=None):
        if cause is not None:
            super(LogHubClientWorkerException, self).__init__(message, cause)
        else:
            super(LogHubClientWorkerException, self).__init__(message)
