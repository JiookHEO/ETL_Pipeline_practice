import os
import json
import requests
from cryptography.fernet import Fernet

def ETL_Pipeline():

    url = 'http://ec2-3-37-12-122.ap-northeast-2.compute.amazonaws.com/api/data/log'
    key = b't-jdqnDewRx9kWithdsTMS21eLrri70TpkMq2A59jX8='
    JSON_LOG_PATH = './json_log.json'

    # api 요청으로 json_log 받기
    def get_json_log(url):
        json_log = requests.get(url).json()
        return json_log

    json_log = get_json_log(url)

    def save_json_log(path, log):
        # path 를 가진 파일(로그가 저장된 파일)이 없을 경우, log 저장할 파일 생성
        if not os.path.isfile(path):
            with open(path, 'w') as f:
                json.dump(log, f, indent = 2)
        # path 를 가진 파일 있을 경우, 해당 파일에 이어서 log 저장
        else:
            with open(path, 'r') as f:
                log = json.load(f)
            with open(path, 'w') as f:
                json.dump(log, f, indent = 2)

    save_json_log(JSON_LOG_PATH, json_log)