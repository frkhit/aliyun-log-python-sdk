# -*- coding:utf-8 -*-
"""
    test consumer
    warning: this program can only work in my computer!
"""
import logging

from aliyun_loghub_tools.loghub.aliyun_config import loghub_accessKey, loghub_logstore, loghub_accessKeyId, \
    loghub_endpoint, loghub_project
from comm import log

log.add_console_handler()
log.set_level(logging.INFO)


def test_cor_consumer():
    from aliyun.log.consumer.config import LoghubCursorPosition
    from aliyun.log.sample import SimpleCorLoghubConsumer
    from aliyun.log.sample.simple_utils import create_guid
    consumer = SimpleCorLoghubConsumer(loghub_logstore, loghub_project,
                                       loghub_endpoint, loghub_accessKeyId,
                                       loghub_accessKey, group="sss1",
                                       consumer_name=create_guid(), heartbeat_interval_in_second=10,
                                       data_fetch_interval_in_second=1,
                                       cursor_position=LoghubCursorPosition.BEGIN_CURSOR, log_queue=None,
                                       can_update_consumer_group=True)
    for msg in consumer:
        log.info("msg key: %s, value: %s, t: %s" % (msg.key, msg.value, msg.time))


if __name__ == '__main__':
    test_cor_consumer()
