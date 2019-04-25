from . import DINGTALK_CONF
import requests
import json

def send_msg(content, msgtype='markdown', contact=None, **kwargs):
    if not DINGTALK_CONF:
        return
    if not contact:
        contact=DINGTALK_CONF['mobile']
    url = 'https://oapi.dingtalk.com/robot/send?access_token=' + DINGTALK_CONF['access_token']
    headers = {'Content-type': 'application/json'}
    data = {
    'msgtype': msgtype,
    msgtype: build_msg_content(content=content, msgtype=msgtype, contact=contact, **kwargs),
    'at': {'atMobiles': [contact]}
    }
    resp = requests.post(url, headers=headers, data=json.dumps(data))
    return resp

def build_msg_content(content, msgtype, title='', msg_url='', pic_url='', contact=None, **kwargs):
    if msgtype == 'markdown':
        data = {'title': title, 'text': content + '\n\n @' + contact}
    elif msgtype == 'text':
        data = {'content': content}
    elif msgtype == 'link':
        data = {'title': title, 'text': content, 'messageUrl': msg_url, 'picUrl': pic_url}
    return data
