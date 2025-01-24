SELECT
    vol.spot_num,
    vol.io_type,
    vol.lane_num,
    vol.vol,
    vol.year,
    vol.month,
    vol.day,
    vol.hour,
    spot.lat,
    spot.lon,
    spot.spot_nm
FROM
    cloud9_transformed.tr_volume vol
JOIN
    cloud9_transformed.spot_info spot
ON
    vol.spot_num = spot.spot_num