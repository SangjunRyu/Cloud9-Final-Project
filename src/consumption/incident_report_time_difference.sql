SELECT 
    r.incident_report_id,
    r.cause,
    r.result,
    r.district_name,
    r.dong_name,
    r.city_rural_category,
    r.latitude,
    r.longitude,
    r.place_name,
    r.place_detail,
    r.cause_subcategory,
    r.fire_station_name,
    r.fire_station_center_name,
    r.report_yr,
    r.report_mnth,
    r.report_day,
    t.report_timestamp,
    t.time_difference
FROM cloud9_mart.mt_incident_time t
JOIN cloud9_mart.mt_incident_report r 
    ON t.incident_report_id = r.incident_report_id
where t.time_difference >= 1
AND t.time_difference <= 15
AND r.latitude > 33