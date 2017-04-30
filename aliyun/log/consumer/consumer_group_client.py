# -*- coding: utf-8 -*-

import json

from aliyun.log.logclient import LogClient
from aliyun.log.consumer.consumer_group import ConsumerGroup
from aliyun.log.consumer.create_consumer_group_request import CreateConsumerGroupRequest
from aliyun.log.consumer.consumer_group_configure_responce import CreateConsumerGroupResponce
from aliyun.log.consumer.update_consumer_group_response import UpdateConsumerGroupResponse
from aliyun.log.consumer.delete_consumer_group_response import DeleteConsumerGroupResponse
from aliyun.log.consumer.list_consumer_group_response import ListConsumerGroupResponse
from aliyun.log.consumer.consumer_group_update_check_point_request import ConsumerGroupUpdateCheckPointRequest
from aliyun.log.consumer.consumer_group_update_check_point_response import ConsumerGroupUpdateCheckPointResponse
from aliyun.log.consumer.consumer_group_get_check_point_request import ConsumerGroupGetCheckPointRequest
from aliyun.log.consumer.consumer_group_get_check_point_response import ConsumerGroupCheckPointResponse
from aliyun.log.consumer.consumer_group_heart_beat_request import ConsumerGroupHeartBeatRequest
from aliyun.log.consumer.consumer_group_heart_beat_response import ConsumerGroupHeartBeatResponse


class LogConsumerClient(LogClient):

    def __init__(self, endpoint, accessKeyId, accessKey, securityToken=None):
        LogClient.__init__(self, endpoint, accessKeyId, accessKey, securityToken)
        # super(LogConsumerClient, self).__init__(endpoint, accessKeyId, accessKey, securityToken)

    def create_consumer_group(self, project, logstore, consumer_group, timeout, in_order=False):
        request = CreateConsumerGroupRequest(project, logstore, ConsumerGroup(consumer_group, timeout, in_order))
        consumer_group = request.consumer_group
        body_str = consumer_group.to_request_json()

        headers = {
            "x-log-bodyrawsize": '0',
            "Content-Type": "application/json"
        }
        params = {}

        project = request.get_project()
        resource = "/logstores/" + request.get_logstore() + "/consumergroups"
        (resp, header) = self._send("POST", project, body_str, resource, params, headers)
        return CreateConsumerGroupResponce(header)

    def update_consumer_group(self, project, logstore, consumer_group, in_order=None, timeout=None):
        if in_order is None and timeout is None:
            raise 'in_order and timeout can\'t all be None'
        elif in_order is not None and timeout is not None:
            body_dict = {
                'order': in_order,
                'timeout': timeout
            }
        elif in_order is not None:
            body_dict = {
                'order': in_order
            }
        else:
            body_dict = {
                'timeout': timeout
            }
        body_str = json.dumps(body_dict)

        headers = {
            "x-log-bodyrawsize": str(len(body_str)),
            "Content-Type": "application/json"
        }
        params = {}
        resource = "/logstores/" + logstore + "/consumergroups/" + consumer_group
        (resp, header) = self._send("PUT", project, body_str, resource, params, headers)
        return UpdateConsumerGroupResponse(header)

    def delete_consumer_group(self, project, logstore, consumer_group):

        headers = {"x-log-bodyrawsize": '0'}
        params = {}

        resource = "/logstores/" + logstore + "/consumergroups/" + consumer_group
        (resp, header) = self._send("DELETE", project, None, resource, params, headers)
        return DeleteConsumerGroupResponse(header)

    def list_consumer_group(self, project, logstore):

        resource = "/logstores/" + logstore + "/consumergroups"
        params = {}
        headers = {}

        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return ListConsumerGroupResponse(resp, header)

    def update_check_point(self, project, logstore, consumer_group, shard, check_point, consumer='', force_success=True):

        request = ConsumerGroupUpdateCheckPointRequest(project, logstore, consumer_group, consumer, shard, check_point, force_success)
        params = request.get_request_params()
        body_str = request.get_request_body()
        headers = {"Content-Type": "application/json"}
        resource = "/logstores/" + logstore + "/consumergroups/" + consumer_group
        (resp, header) = self._send("POST", project, body_str, resource, params, headers)
        return ConsumerGroupUpdateCheckPointResponse(header)

    def get_check_point(self, project, logstore, consumer_group, shard=-1):
        request = ConsumerGroupGetCheckPointRequest(project, logstore, consumer_group, shard)
        params = request.get_params()
        headers = {}
        resource = "/logstores/" + logstore + "/consumergroups/" + consumer_group
        (resp, header) = self._send("GET", project, None, resource, params, headers)
        return ConsumerGroupCheckPointResponse(resp, header)

    def heart_beat(self, project, logstore, consumer_group, consumer, shards=None):
        if shards is None:
            shards = []
        request = ConsumerGroupHeartBeatRequest(project, logstore, consumer_group, consumer, shards)
        body_str = request.get_request_body()
        params = request.get_params()
        headers = {"Content-Type": "application/json"}
        resource = "/logstores/" + logstore + "/consumergroups/" + consumer_group
        (resp, header) = self._send('POST', project, body_str, resource, params, headers)
        return ConsumerGroupHeartBeatResponse(resp, header)