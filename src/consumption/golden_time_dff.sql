SELECT 
    t.incident_report_id, 
    r.cause,
    r.result,
    t.report_timestamp, 
    t.report_time,
    t.arrival_timestamp, 
    t.arrival_time,
    t.time_difference, 
    r.district_name,
    r.dong_name,
    fire_station_name,
    fire_station_center_name
FROM cloud9_mart.mt_incident_time t
JOIN cloud9_mart.mt_incident_report r 
    ON t.incident_report_id = r.incident_report_id
WHERE t.time_difference >= 1 
  AND t.time_difference <= 15
  AND r.fire_station_name IN (
    '강남소방서', '구로소방서', '서초소방서', '송파소방서', '영등포소방서', 
    '마포소방서', '강서소방서', '관악소방서', '강동소방서', '양천소방서', 
    '동대문소방서', '은평소방서', '노원소방서', '성북소방서', '동작소방서', 
    '강북소방서', '종로소방서', '중랑소방서', '중부소방서', '도봉소방서', 
    '서대문소방서', '광진소방서', '용산소방서', '성동소방서', '금천소방서'
  )