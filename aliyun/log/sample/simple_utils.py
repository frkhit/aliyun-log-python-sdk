# -*- coding:utf-8 -*-


def json2pyobj(json_str, encoding="utf-8"):
    import json
    return json.loads(json_str, encoding=encoding)


def pyobj2json(py_obj, encoding="utf-8"):
    import json
    return json.dumps(py_obj, encoding=encoding)


def create_guid():
    import uuid
    return str(uuid.uuid1()).decode('utf-8').lower()


def get_log_key():
    return "log"
