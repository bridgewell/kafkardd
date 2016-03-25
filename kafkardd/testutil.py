# -*- coding: utf-8 -*-
import json

def generate_message(partition, offset, timestamp):
    msg = {
        'partition': partition,
        'offset': offset,
        'timestamp': timestamp
    }
    return json.dumps(msg)

def validate_message(message, partition=None, offset=None, timestamp=None):
    msg = json.loads(message)
    if partition and msg['partition'] != partition:
        return False
    if offset and msg['offset'] != offset:
        return False
    if timestamp and msg['timestamp'] != timestamp:
        return False
    return True

def extract_timestamp_from_message(message):
    msg = json.loads(message)
    if 'timestamp' in msg:
        return msg['timestamp']
    return None
