SELECT *
FROM cloud9_transformed.tr_velocity
WHERE 
  year = CAST(year(current_timestamp + interval '9' hour) AS VARCHAR)
  AND month = LPAD(CAST(month(current_timestamp + interval '9' hour) AS VARCHAR), 2, '0')
  AND day = LPAD(CAST(day(current_timestamp + interval '9' hour) AS VARCHAR), 2, '0')
  AND hour = LPAD(CAST(hour(current_timestamp + interval '9' hour) AS VARCHAR), 2, '0')