-- Quicksight SQL for getting accident real time info(UTC 9)
-- Inner join with link(link_id)

SELECT 
    MAX(acc_id) AS acc_id,
    occr_time,
    occurred_at,
    exp_clr_date,
    exp_clr_time,
    cleared_at,
    acc_type,
    acc_dtype,
    link_id,
    latitude,
    longitude,
    acc_info,
    occr_date
FROM 
    cloud9_transformed.tr_accident
WHERE 
    occr_date = DATE_FORMAT(current_timestamp + INTERVAL '9' HOUR, '%Y%m%d')
AND cleared_at > (current_timestamp + INTERVAL '9' HOUR)
AND latitude > 36
GROUP BY 
    occr_time,
    occurred_at,
    exp_clr_date,
    exp_clr_time,
    cleared_at,
    acc_type,
    acc_dtype,
    link_id,
    latitude,
    longitude,
    acc_info,
    occr_date
ORDER BY 
    acc_id ASC