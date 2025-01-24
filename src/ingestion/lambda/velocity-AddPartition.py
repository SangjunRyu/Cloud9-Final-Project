import boto3
from datetime import datetime, timedelta

def lambda_handler(event, context):
    # Athena 및 S3 관련 설정
    athena_client = boto3.client('athena', region_name='ap-northeast-2')  # 리전을 적절히 설정
    database_name = 'cloud9_transformed'  # Athena 데이터베이스 이름
    table_name = 'tr_velocity'        # Athena 테이블 이름
    s3_output = 's3://aws-athena-query-results-ap-northeast-2-572591758643'  # Athena 쿼리 결과를 저장할 S3 경로

    # 현재 서울 시간 (UTC+9)
    current_time = datetime.utcnow() + timedelta(hours=9)
    year = current_time.strftime('%Y')
    month = current_time.strftime('%m')
    day = current_time.strftime('%d')
    hour = current_time.strftime('%H')

    # S3 데이터 경로 (새 파티션 정보)
    partition_path = f"s3://cloud9-transformed/traffic_velocity_transformed/year={year}/month={month}/day={day}/hour={hour}/"

    # ALTER TABLE ADD PARTITION 쿼리 생성
    query = f"""
    ALTER TABLE {database_name}.{table_name}
    ADD IF NOT EXISTS 
    PARTITION (year='{year}', month='{month}', day='{day}', hour='{hour}')
    LOCATION '{partition_path}';
    """
    print("Generated Query:", query)

    # Athena에 쿼리 실행
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': s3_output
        }
    )

    # 쿼리 실행 ID 반환
    query_execution_id = response['QueryExecutionId']
    print(f"Query Execution ID: {query_execution_id}")

    return {
        'statusCode': 200,
        'body': f"Partition added for {year}-{month}-{day} {hour}:00"
    }
