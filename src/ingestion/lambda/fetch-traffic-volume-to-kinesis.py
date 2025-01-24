import boto3
import requests
import xml.etree.ElementTree as ET
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time

# API 호출 함수
def fetch_traffic_volume(api_key, service, start_index, end_index, spot_num, ymd, hh):
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/{service}/{start_index}/{end_index}/{spot_num}/{ymd}/{hh}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(f"API 요청 실패: {response.status_code} (SPOT_NUM: {spot_num})")
        return None

# XML 데이터 파싱 함수
def parse_traffic_volume(xml_data):
    root = ET.fromstring(xml_data)
    rows = root.findall(".//row")
    data = []
    for row in rows:
        data.append({
            "SPOT_NUM": row.find("spot_num").text,
            "IO_TYPE": row.find("io_type").text,
            "LANE_NUM": row.find("lane_num").text,
            "VOL": row.find("vol").text
        })
    return data

# 데이터 존재 여부 확인 함수
def is_data_available(xml_data):
    root = ET.fromstring(xml_data)
    result_code = root.find(".//RESULT/CODE").text
    return result_code != "INFO-200"

# Lambda 핸들러 함수
def lambda_handler(event, context):
    # 환경 변수 및 설정
    API_KEY = "706e72417179736a37346c72587565"
    SERVICE = "VolInfo"
    START_INDEX = 1
    END_INDEX = 1000
    KINESIS_STREAM_NAME = "traffic_volume"
    REGION_NAME = "ap-northeast-2"

    # 현재 서울 시간을 기준으로 1시간 전 시간 계산
    one_hour_ago = datetime.now(ZoneInfo("Asia/Seoul")) - timedelta(hours=1)
    ymd = one_hour_ago.strftime("%Y%m%d")
    hh = one_hour_ago.strftime("%H")
    
    # Kinesis 클라이언트 생성
    kinesis_client = boto3.client("kinesis", region_name=REGION_NAME)

    # spots.csv 파일의 SPOT_NUM 정보
    spots = [
        "A-01", "A-02", "A-03", "A-04", "A-05", "A-06", "A-07", "A-08", "A-09", "A-10",
        "A-11", "A-12", "A-13", "A-14", "A-15", "A-16", "A-17", "A-18", "A-19", "A-20",
        "A-21", "A-22", "A-23", "A-24", "B-01", "B-02", "B-03", "B-04", "B-05", "B-06",
        "B-07", "B-08", "B-09", "B-10", "B-11", "B-12", "B-13", "B-14", "B-15", "B-16",
        "B-17", "B-18", "B-19", "B-20", "B-21", "B-22", "B-23", "B-24", "B-25", "B-26",
        "B-27", "B-28", "B-29", "B-30", "B-31", "B-32", "B-33", "B-34", "B-35", "B-36",
        "B-37", "B-38", "C-01", "C-02", "C-03", "C-04", "C-05", "C-06", "C-07", "C-08",
        "C-09", "C-10", "C-11", "C-12", "C-13", "C-14", "C-15", "C-16", "C-17", "C-18",
        "C-19", "C-20", "C-21", "D-01", "D-02", "D-03", "D-04", "D-05", "D-06", "D-07",
        "D-08", "D-09", "D-10", "D-11", "D-12", "D-13", "D-14", "D-15", "D-16", "D-17",
        "D-18", "D-19", "D-20", "D-21", "D-22", "D-23", "D-24", "D-25", "D-26", "D-27",
        "D-28", "D-29", "D-30", "D-31", "D-32", "D-33", "D-34", "D-35", "D-36", "D-37",
        "D-38", "D-39", "D-40", "D-41", "D-42", "D-43", "D-44", "D-45", "D-46", "F-01",
        "F-02", "F-03", "F-04", "F-05", "F-06", "F-07", "F-08", "F-09", "F-10"
    ]

    # 각 SPOT_NUM에 대해 API 호출 및 데이터 전송
    for spot_num in spots:
        retry_count = 0
        while retry_count < 3:
            response = fetch_traffic_volume(API_KEY, SERVICE, START_INDEX, END_INDEX, spot_num, ymd, hh)
            if response and is_data_available(response):
                parsed_data = parse_traffic_volume(response)
                for record in parsed_data:
                    kinesis_client.put_record(
                        StreamName=KINESIS_STREAM_NAME,
                        Data=json.dumps(record),
                        PartitionKey=record["SPOT_NUM"]
                    )
                    print(f"Sent data to Kinesis: {record}")
                break  # 데이터 처리 성공 시 루프 탈출
            else:
                print(f"데이터 없음. 재시도 {retry_count + 1}/3 (SPOT_NUM: {spot_num})")
                retry_count += 1
                time.sleep(5)

    return {"statusCode": 200, "body": "Data successfully sent to Kinesis"} 