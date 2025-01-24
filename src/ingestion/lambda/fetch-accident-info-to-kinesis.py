import boto3
import requests
import xml.etree.ElementTree as ET
import json
import os
import sys
from datetime import datetime

# Kinesis 스트림 이름
KINESIS_STREAM_NAME = 'accident'

# AWS Kinesis 클라이언트
kinesis_client = boto3.client('kinesis')

# API 호출 함수
def fetch_accident_info(api_key, service, start_index, end_index):
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/{service}/{start_index}/{end_index}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text  # XML 데이터를 반환
    else:
        print(f"API 요청 실패: {response.status_code}")
        return None

# XML 데이터 파싱 함수
def parse_accident_info(xml_data):
    root = ET.fromstring(xml_data)  # XML 데이터 파싱
    rows = root.findall(".//row")
    
    # 데이터를 저장할 리스트
    data = []
    for row in rows:
        try:
            # Watermark를 위한 컬럼 생성
            occr_date = row.find("occr_date").text
            occr_time = row.find("occr_time").text
            occured_at = None

            if occr_date and occr_time:
                # occr_time이 6자리인 경우 앞 4자리만 사용
                occr_time = occr_time[:4] if len(occr_time) >= 4 else occr_time
                dt = datetime.strptime(occr_date + " " + occr_time, "%Y%m%d %H%M")
                # 밀리초 포함 형식으로 변환
                occured_at = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 초 뒤에 3자리 밀리초 포함

            # Watermark를 위한 컬럼 생성
            exp_clr_date = row.find("exp_clr_date").text
            exp_clr_time = row.find("exp_clr_time").text
            cleared_at = None

            if exp_clr_date and exp_clr_time:
                # exp_clr_time이 6자리인 경우 앞 4자리만 사용
                exp_clr_time = exp_clr_time[:4] if len(exp_clr_time) >= 4 else exp_clr_time
                dt = datetime.strptime(exp_clr_date + " " + exp_clr_time, "%Y%m%d %H%M")
                # 밀리초 포함 형식으로 변환
                cleared_at = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 초 뒤에 3자리 밀리초 포함
            
            data.append({
                "ACC_ID": row.find("acc_id").text,
                "OCCR_DATE": occr_date,
                "OCCR_TIME": occr_time,
                "OCCURRED_AT": occured_at,
                "EXP_CLR_DATE": exp_clr_date,
                "EXP_CLR_TIME": exp_clr_time,
                "CLEARED_AT": cleared_at,
                "ACC_TYPE": row.find("acc_type").text,
                "ACC_DTYPE": row.find("acc_dtype").text,
                "LINK_ID": row.find("link_id").text,
                "GRS80TM_X": row.find("grs80tm_x").text,
                "GRS80TM_Y": row.find("grs80tm_y").text,
                "ACC_INFO": row.find("acc_info").text.strip(),
                "ACC_ROAD_CODE": row.find("acc_road_code").text if row.find("acc_road_code") is not None else None
            })
        except AttributeError as e:
            print(f"데이터 파싱 실패: {e}, 데이터: {ET.tostring(row, encoding='utf8').decode('utf8')}")
    return data

# Kinesis로 데이터 전송 (put_records 방식)
def send_records_to_kinesis(data):
    # 레코드 구성
    records = [
        {
            'Data': json.dumps(record),  # JSON 직렬화
            'PartitionKey': record['ACC_ID']  # ACC_ID를 파티션 키로 사용
        }
        for record in data
    ]

    # 데이터 크기 계산
    total_size = sum(sys.getsizeof(json.dumps(record)) for record in data)
    print(f"총 데이터 크기: {total_size} 바이트")
    
    # Kinesis로 데이터 전송
    response = kinesis_client.put_records(
        StreamName=KINESIS_STREAM_NAME,
        Records=records
    )

    # 결과 확인
    failed_count = response['FailedRecordCount']
    if failed_count > 0:
        print(f"전송 실패: {failed_count}건")
        for idx, record in enumerate(response['Records']):
            if 'ErrorCode' in record:
                print(f"실패한 레코드: {data[idx]['ACC_ID']}, 이유: {record['ErrorCode']}")
    else:
        print("모든 레코드 전송 성공")

    return {
        "total_records": len(records),
        "total_size": total_size,
        "failed_count": failed_count
    }

# Lambda 핸들러
def lambda_handler(event, context):
    API_KEY = "706e72417179736a37346c72587565"  # 환경 변수에서 API 키 가져오기
    SERVICE = "AccInfo"
    START_INDEX = 1
    END_INDEX = 1000

    # API 호출
    api_response = fetch_accident_info(API_KEY, SERVICE, START_INDEX, END_INDEX)
    if not api_response:
        print("API 호출 실패")
        return {"statusCode": 500, "body": "API 호출 실패"}

    # XML 데이터를 파싱하여 JSON 형태로 변환
    parsed_data = parse_accident_info(api_response)

    # 데이터 개수 확인
    print(f"파싱된 데이터 개수: {len(parsed_data)}")
    if not parsed_data:
        print("API에서 가져온 데이터가 없습니다.")
        return {"statusCode": 200, "body": "API에서 가져온 데이터가 없습니다."}

    # 데이터 전송
    result = send_records_to_kinesis(parsed_data)
    
    print(f"전송 결과: 총 {result['total_records']}건 전송, 총 데이터 크기 {result['total_size']} 바이트, 실패 {result['failed_count']}건")
    
    return {
        "statusCode": 200,
        "body": f"데이터 처리 완료. 총 {result['total_records']}건 전송, 총 데이터 크기 {result['total_size']} 바이트, 실패 {result['failed_count']}건"
    }