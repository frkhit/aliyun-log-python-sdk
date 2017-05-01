# -*- coding:utf-8 -*-
"""
    test producer
    warning: this program can only work in my computer!
"""

import logging

from comm import log

log.add_console_handler()
log.set_level(logging.INFO)


def test_producer():
    import time

    from aliyun_loghub_tools.loghub import aliyun_config
    from aliyun_loghub_tools.loghub.aliyun_config import loghub_logstore
    from aliyun.log.sample import SimpleLoghubProducer

    producer = SimpleLoghubProducer(aliyun_config.loghub_project, aliyun_config.loghub_endpoint,
                                    aliyun_config.loghub_accessKeyId, aliyun_config.loghub_accessKey,
                                    create_logstore_if_not_exists=False,
                                    ttl_days_of_new_logstore=1, shard_count_of_new_logstore=2)
    count = 0
    while True:
        # value = "value%s" % create_guid()
        producer.send('A', value="v %d" % count, key="k %d" % count, logstore=loghub_logstore)
        log.info("sending msg v %d to A" % count)
        count += 1

        time.sleep(0.5)


if __name__ == '__main__':
    test_producer()
