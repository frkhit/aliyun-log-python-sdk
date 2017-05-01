# -*- coding:utf-8 -*-
"""
    test consumer
    warning: this program can only work in my computer!
"""
import logging

from comm import log

log.add_console_handler()
log.set_level(logging.INFO)


def test_cor_consumer():
    from aliyun_loghub_tools.loghub import aliyun_config
    from aliyun.log.consumer.config import LoghubCursorPosition
    from aliyun.log.sample import SimpleCorLoghubConsumer
    from aliyun.log.sample.simple_utils import create_guid
    consumer = SimpleCorLoghubConsumer(aliyun_config.loghub_logstore, aliyun_config.loghub_project, group="sss1",
                                       consumer_name=create_guid(),
                                       heartbeat_interval_in_second=10,
                                       cursor_position=LoghubCursorPosition.BEGIN_CURSOR)
    for msg in consumer:
        log.info("msg key: %s, value: %s, t: %s" % (msg.key, msg.value, msg.time))


if __name__ == '__main__':
    test_cor_consumer()
