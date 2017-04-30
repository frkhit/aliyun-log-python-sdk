# -*- coding: utf-8 -*-


import time
import logging
from threading import Thread

from aliyun.log.logexception import LogException

from aliyun.log.consumer.loghub_consumer import LoghubConsuemr
from aliyun.log.consumer.loghub_client_adapter import LogHubClientAdapter
from aliyun.log.consumer.loghub_heart_beat import LoghubHeartBeat
from aliyun.log.consumer.loghub_exceptions.loghub_client_worker_exception import LogHubClientWorkerException


class ClientWorker(Thread):

    def __init__(self, factory, loghub_config):
        super(ClientWorker, self).__init__()
        self.loghub_processor_factory = factory
        self.loghub_config = loghub_config
        self.loghub_client_adapter = \
            LogHubClientAdapter(loghub_config.endpoint, loghub_config.accessKeyId, loghub_config.accessKey,
                                loghub_config.project, loghub_config.logstore, loghub_config.consumer_group_name,
                                loghub_config.consumer_name, loghub_config.securityToken)
        self.shut_down_flag = False
        self.logger = logging.getLogger(self.__class__.__name__)
        self.shard_consumer = {}

        try:
            self.loghub_client_adapter.create_consumer_grouop(loghub_config.heartbeat_interval, loghub_config.in_order)

        except LogException as e:
            # consumer group already exist
            if e.get_error_code() == 'ConsumerGroupAlreadyExist':

                try:
                    consumer_group = self.loghub_client_adapter.get_consumer_group()
                    # consumer group is not in server
                    if consumer_group is None:
                        raise LogHubClientWorkerException('consumer group not exist')
                    # the consuemr group's attribute(in_order or timeout) is different from the server's
                    if consumer_group is not None and (consumer_group.is_in_order() != loghub_config.in_order
                                                       or consumer_group.get_timeout() != loghub_config.heartbeat_interval):
                            raise LogHubClientWorkerException(
                                "consumer group is not agreed, AlreadyExistedConsumerGroup: {\"consumeInOrder\": " +
                                str(consumer_group.is_in_order()) + ", \"timeoutInMillSecond\": " +
                                str(consumer_group.get_timeout()) + "}")
                except LogException as e1:
                    raise LogHubClientWorkerException("error occour when get consumer group, errorCode: " +
                                                      e1.get_error_code() + ", errorMessage: " + e1.get_error_message())

            else:
                raise LogHubClientWorkerException('error occour when create consumer group, errorCode: '
                                                  + e.get_error_code() + ", errorMessage: " + e.get_error_message())

        self.loghub_heart_beat = LoghubHeartBeat(self.loghub_client_adapter, loghub_config.heartbeat_interval)

    def switch_client(self, accessKeyId, accessKey, securityToken=None):
        self.loghub_client_adapter.swith_client(self.loghub_config.endpoint, accessKeyId, accessKey, securityToken)

    def run(self):
        logging.debug('worker start')
        self.loghub_heart_beat.start()
        while not self.shut_down_flag:
            held_shards = self.loghub_heart_beat.get_held_shards()
            for shard in held_shards:
                consumer = self._get_consumer(shard)
                consumer.consume()
            self.clean_consumer(held_shards)
            try:
                time.sleep(self.loghub_config.data_fetch_interval)
            except Exception as e:
                print e

    def clean_consumer(self, owned_shards):
        remove_shards = []
        # 将服务器不分配的shard移除
        for shard, consumer in self.shard_consumer.items():
            if shard not in owned_shards:
                consumer.shut_down()
                self.logger.warning('Try to shut down consumer shard: ' + str(shard))
            if consumer.is_shutdown():
                self.loghub_heart_beat.remove_heart_shard(shard)
                remove_shards.append(shard)
                self.logger.warning('Remove a consumer shard:' + str(shard))

        for shard in remove_shards:
            self.shard_consumer.pop(shard)

    def shutdown(self):
        self.shut_down_flag = True
        self.loghub_heart_beat.shutdown()
        logging.debug('worker stop')

    def _get_consumer(self, shard_id):
        consumer = self.shard_consumer.get(shard_id, None)
        if consumer is not None:
            return consumer
        consumer = LoghubConsuemr(self.loghub_client_adapter, shard_id, self.loghub_config.consumer_name,
                                  self.loghub_processor_factory.generate_processor(),
                                  self.loghub_config.cursor_position, self.loghub_config.cursor_start_time)
        self.shard_consumer[shard_id] = consumer
        return consumer





