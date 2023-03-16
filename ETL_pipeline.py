import os
import json
import requests
import uuid
import base64
from cryptography.fernet import Fernet
from b64uuid import B64UUID

def ETL_Pipeline():

    url = 'http://ec2-3-37-12-122.ap-northeast-2.compute.amazonaws.com/api/data/log'
    key = b't-jdqnDewRx9kWithdsTMS21eLrri70TpkMq2A59jX8='
    JSON_LOG_PATH = './json_log.json'

    # api 요청으로 json_log 받기
    def get_json_log(url):
        json_log = requests.get(url).json()
        return json_log

    json_log = get_json_log(url)

    # 이미 이전에 받은 데이터는 제외하고 새로운 데이터만 반환
    def except_duplicate(new, log_exist):
        for i in log_exist:
            if i == new:
                return False
        return True

    # 받은 json_log 저장
    def save_json_log(path, log):
        # path 를 가진 파일(로그가 저장된 파일)이 없을 경우, log 저장할 파일 생성
        if not os.path.isfile(path):
            with open(path, 'w') as f:
                json.dump(log, f, indent = 2)
        # path 를 가진 파일 있을 경우, 해당 파일에 이어서 log 저장
        # 중복 제외, 새로운 log 만 저장
        else:
            with open(path, 'r') as f:
                log_exist = json.load(f)
            for i in log:
                if except_duplicate(i, log_exist):
                    log_exist.append(i)
            with open(path, 'w') as f:
                json.dump(log, f, indent = 2)

    save_json_log(JSON_LOG_PATH, json_log)


    # 받은 log의 암호화된 데이터 복호화, 문자열 압축 알고리즘 적용 후 다시 암호화 및 저장
    # 1. 암호화된 부분 복호화
    # 2. b64uuid 모듈을 사용하여 'user_id' 64(32 / 32) 자 -> 44(22 /22) 자 압축
    # 3. 'method' 를 value 별 숫자로 압축
    # 4. 'url' 을 value 별 숫자로 압축
    # 5. 'inDate' 다른 표기방식으로 압축
    # 6. 암호화
    # 7. 파일 저장(.json)


    # 1. 암호화된 부분 복호화
    def decrypt_data(log, key):
        fernet = Fernet(key)
        for i in log:
            decrypted_data = fernet.decrypt(i['data']).decode('ascii')
            i['data'] = decrypted_data
            # replace 로 ' -> " 를 변환해주는 이유는 json 모듈이 key 나 value 를 문자열로 인식하기 때문
            # json 모듈의 인식가능한 문자열은 반드시 ""로 감싸여져 있어야 한다(JavaScript 에서 유래된 모듈이다)
            # 문자열로 이루어진 'data' 의 복호화된 문자열 내부 data를 dict 로 변환
            i['data'] = json.loads(i['data'].replace("'", "\""))
        return
    
    # 2. b64uuid 모듈을 사용하여 'user_id' 64(32 / 32) 자 -> 44(22 /22) 자 압축
    def to_b64uuid(log):
        for i in log:
            i['data']['user_id'] = B64UUID(i['data']['user_id'][:32]).string + B64UUID(i['data']['user_id'][32:]).string
        return


    decrypt_data(json_log, key)
    to_b64uuid(json_log)