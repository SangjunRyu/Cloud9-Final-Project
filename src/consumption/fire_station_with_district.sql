SELECT 
    fire_station_name,
    address,
    phone,
    fax,
    latitude,
    longitude,
    fire_station_center_name,
    REGEXP_EXTRACT(address, '([^ ]+구)') AS district_name
FROM cloud9_mart.mt_fire_station
WHERE fire_station_center_name LIKE '%안전센터%'