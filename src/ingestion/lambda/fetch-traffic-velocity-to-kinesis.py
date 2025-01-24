import awswrangler as wr
import boto3
import random
import requests
import xml.etree.ElementTree as ET
import json
import time

# S3에서 Parquet 데이터를 읽어오는 함수
def fetch_link_info_from_s3(bucket_name, object_key):
    path = f"s3://{bucket_name}/{object_key}"
    try:
        df = wr.s3.read_parquet(path=path)
        print(f"Dataframe Columns: {df.columns}")
        print(f"Sample Dataframe: {df.head(5)}")
        return df
    except Exception as e:
        print(f"Error reading Parquet data: {e}")
        raise

# 지역구별 비율 기반 샘플링
def sample_links_by_district(data, target_sample_size):
    try:
        district_counts = data['district'].value_counts()
        total_links = district_counts.sum()
        district_proportions = (district_counts / total_links).to_dict()

        sampled_links = []
        for district, proportion in district_proportions.items():
            district_links = data[data['district'] == district]['LINK_ID'].tolist()
            sample_size = max(1, int(proportion * target_sample_size))
            sampled_links.extend(random.sample(district_links, min(sample_size, len(district_links))))
        return sampled_links
    except Exception as e:
        print(f"Sampling Error: {e}")
        raise

# API 호출 함수
def fetch_traffic_info(api_key, service, start_index, end_index, link_id):
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/{service}/{start_index}/{end_index}/{link_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(f"API 요청 실패: {response.status_code} (LINK_ID: {link_id})")
        return None

# XML 데이터 파싱 함수
def parse_traffic_info(xml_data):
    root = ET.fromstring(xml_data)
    rows = root.findall(".//row")
    data = []
    for row in rows:
        data.append({
            "PRCS_SPD": row.find("prcs_spd").text,
            "PRCS_TRV_TIME": row.find("prcs_trv_time").text
        })
    return data

# Kinesis 데이터 전송 함수
def send_to_kinesis(kinesis_client, stream_name, record):
    try:
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(record),
            PartitionKey=str(record["link_id"])  # link_id를 문자열로 변환
        )
        print(f"Sent data to Kinesis: {record}")
    except Exception as e:
        print(f"Error sending to Kinesis: {e}")
        raise

# Lambda 핸들러 함수
def lambda_handler(event, context):
    API_KEY = "706e72417179736a37346c72587565"
    SERVICE = "TrafficInfo"
    START_INDEX = 1
    END_INDEX = 5
    S3_BUCKET_NAME = "ysj-traffic-info"
    S3_OBJECT_KEY = "link/link.parquet"
    TARGET_SAMPLE_SIZE = 100
    KINESIS_STREAM_NAME = "traffic_velocity"
    REGION_NAME = "ap-northeast-2"

    # Kinesis 클라이언트 생성
    kinesis_client = boto3.client("kinesis", region_name=REGION_NAME)

    try:
        # S3에서 링크 데이터 가져오기
        link_data = fetch_link_info_from_s3(S3_BUCKET_NAME, S3_OBJECT_KEY)
        sampled_link_ids = sample_links_by_district(link_data, TARGET_SAMPLE_SIZE)
        print(f"샘플링된 LINK_ID 수: {len(sampled_link_ids)}")
    except Exception as e:
        print(f"Error during data fetch or sampling: {e}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}

    success_count = 0
    failure_count = 0

    for link_id in sampled_link_ids:
        try:
            # 링크 정보 조회
            base_info = link_data[link_data['LINK_ID'] == link_id]
            if base_info.empty:
                print(f"링크 정보 없음 (LINK_ID: {link_id})")
                failure_count += 1
                continue  # 다음 링크로 진행

            base_info = base_info.iloc[0].to_dict()
            response = fetch_traffic_info(API_KEY, SERVICE, START_INDEX, END_INDEX, link_id)

            if response:
                parsed_data = parse_traffic_info(response)
                for data in parsed_data:
                    full_record = {
                        "link_id": base_info["LINK_ID"],
                        "lat": base_info["lat"],
                        "lon": base_info["lon"],
                        "district": base_info["district"],
                        "subdistrict": base_info["subdistrict"],
                        "PRCS_SPD": data["PRCS_SPD"],
                        "PRCS_TRV_TIME": data["PRCS_TRV_TIME"]
                    }
                    send_to_kinesis(kinesis_client, KINESIS_STREAM_NAME, full_record)
                    success_count += 1
            else:
                print(f"API 데이터 없음 (LINK_ID: {link_id})")
                failure_count += 1

        except Exception as e:
            print(f"예외 발생: {e} (LINK_ID: {link_id})")
            failure_count += 1

        # 0.2초 대기 (API 호출 제한 방지)
        time.sleep(0.2)

    print(f"성공 호출 수: {success_count}")
    print(f"실패 호출 수: {failure_count}")

    return {
        "statusCode": 200,
        "body": {
            "message": "Processing completed",
            "success_count": success_count,
            "failure_count": failure_count
        }
    }