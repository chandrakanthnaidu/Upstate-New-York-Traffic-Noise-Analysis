-- 1. Get all available months for month selector (No Arguments)
-- Returns distinct months from monthly_summary for populating the month dropdown
SELECT DISTINCT month
FROM monthly_summary
ORDER BY month;

-- 2. Get top 100 max dBA records with images for a month (month: VARCHAR, e.g., '2025-04')
-- Returns traffic_id, max_dba, and debug_img for the top 100 records in the specified month
SELECT a.traffic_id, a.max_dba, t.debug_img
FROM AudioData a
JOIN TrafficData t ON a.traffic_id = t.traffic_id
WHERE DATE_FORMAT(t.dto, '%Y-%m') = %s
ORDER BY a.max_dba DESC
LIMIT 100;

-- 3. Get total vehicle count for a month (month: VARCHAR, e.g., '2025-04')
-- Returns the sum of vehicle_count for the specified month
SELECT SUM(vehicle_count) AS vehicle_count
FROM monthly_summary 
WHERE month = %s;

-- 4. Get daily data for month graph (month: VARCHAR, e.g., '2025-04')
-- Returns day, max_dba, and vehicle_count for each day in the specified month
SELECT day, max_dba, vehicle_count
FROM monthly_summary 
WHERE month = %s
ORDER BY day;

-- 5. Get all available dates for date selector (No Arguments)
-- Returns distinct dates from daily_summary for populating the date dropdown
SELECT DISTINCT date
FROM daily_summary
ORDER BY date;

-- 6. Get top 100 max dBA records with images for a date (date: DATE, e.g., '2025-04-25')
-- Returns traffic_id, max_dba, and debug_img for the top 100 records on the specified date
SELECT a.traffic_id, a.max_dba, t.debug_img
FROM AudioData a
JOIN TrafficData t ON a.traffic_id = t.traffic_id
WHERE DATE(t.dto) = %s
ORDER BY a.max_dba DESC
LIMIT 100;

-- 7. Get total vehicle count for a date (date: DATE, e.g., '2025-04-25')
-- Returns the sum of vehicle_count for the specified date
SELECT SUM(vehicle_count) AS vehicle_count
FROM daily_summary 
WHERE date = %s;

-- 8. Get 10-minute interval data for day graph (date: DATE, e.g., '2025-04-25')
-- Returns hour, ten_min_interval, max_dba, and vehicle_count for the specified date
SELECT hour, ten_min_interval, max_dba, vehicle_count
FROM daily_summary 
WHERE date = %s
ORDER BY hour, ten_min_interval;

-- 9. Get image details by traffic_id (traffic_id: INT)
-- Returns traffic_id, max_dba, dto, and debug_img for the specified traffic_id
SELECT t.traffic_id, a.max_dba, t.dto, t.debug_img
FROM TrafficData t
JOIN AudioData a ON t.traffic_id = a.traffic_id
WHERE t.traffic_id = %s;

-- 10. Get maximum traffic_id (No Arguments)
-- Returns the highest traffic_id in TrafficData for assigning new IDs
SELECT MAX(traffic_id) as max_id
FROM TrafficData;

-- 11. Create TrafficData table (No Arguments)
-- Creates the TrafficData table with specified columns and primary key
CREATE TABLE TrafficData (
    traffic_id INT NOT NULL,
    cam VARCHAR(50),
    probs FLOAT,
    cls INT,
    dto DATETIME,
    save_dto DATETIME,
    point_len INT,
    intersection_x INT,
    intersection_y INT,
    box_x1 FLOAT,
    box_y1 FLOAT,
    box_x2 FLOAT,
    box_y2 FLOAT,
    frame_dto DATETIME,
    tid INT,
    seq_len INT,
    full_img VARCHAR(500),
    debug_img VARCHAR(500),
    PRIMARY KEY(traffic_id)
);

-- 12. Create AudioData table (No Arguments)
-- Creates the AudioData table with specified columns, indexes, and foreign key
CREATE TABLE AudioData (
    audio_id INT NOT NULL AUTO_INCREMENT,
    traffic_id INT,
    snd_file VARCHAR(255),
    snd_lvl FLOAT,
    ks TIME,
    ke TIME,
    kd INT,
    dba1 FLOAT, dba2 FLOAT, dba3 FLOAT, dba4 FLOAT, dba5 FLOAT, dba6 FLOAT,
    dba7 FLOAT, dba8 FLOAT, dba9 FLOAT, dba10 FLOAT, dba11 FLOAT, dba12 FLOAT,
    dba13 FLOAT, dba14 FLOAT, dba15 FLOAT, dba16 FLOAT, dba17 FLOAT, dba18 FLOAT,
    dba19 FLOAT, dba20 FLOAT, dba21 FLOAT, dba22 FLOAT, dba23 FLOAT, dba24 FLOAT,
    dba25 FLOAT, dba26 FLOAT, dba27 FLOAT, dba28 FLOAT, dba29 FLOAT, dba30 FLOAT,
    max_dba DECIMAL(10,2),
    PRIMARY KEY(audio_id),
    INDEX idx_traffic_id (traffic_id),
    INDEX idx_max_dba (max_dba),
    FOREIGN KEY (traffic_id) REFERENCES TrafficData(traffic_id) ON DELETE CASCADE
);

-- 13. Create monthly_summary table (No Arguments)
-- Creates the monthly_summary table for monthly aggregations
CREATE TABLE monthly_summary (
    month VARCHAR(7) NOT NULL,
    day INT NOT NULL,
    vehicle_count INT,
    max_dba DECIMAL(10,2),
    PRIMARY KEY (month, day)
);

-- 14. Create daily_summary table (No Arguments)
-- Creates the daily_summary table for daily aggregations
CREATE TABLE daily_summary (
    date DATE NOT NULL,
    hour INT NOT NULL,
    ten_min_interval INT NOT NULL,
    vehicle_count INT,
    max_dba DECIMAL(10,2),
    PRIMARY KEY (date, hour, ten_min_interval)
);

-- 15. Insert into TrafficData (Multiple Arguments)
-- Inserts a record into TrafficData with specified values
INSERT INTO TrafficData (
    traffic_id, cam, probs, cls, dto, save_dto, point_len, intersection_x, intersection_y, 
    box_x1, box_y1, box_x2, box_y2, frame_dto, tid, seq_len, full_img, debug_img
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);

-- 16. Insert into AudioData (Multiple Arguments)
-- Inserts a record into AudioData with specified values
INSERT INTO AudioData (
    traffic_id, snd_file, snd_lvl, ks, ke, kd, 
    dba1, dba2, dba3, dba4, dba5, dba6, dba7, dba8, dba9, dba10,
    dba11, dba12, dba13, dba14, dba15, dba16, dba17, dba18, dba19, dba20,
    dba21, dba22, dba23, dba24, dba25, dba26, dba27, dba28, dba29, dba30, max_dba
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);

-- 17. Insert into monthly_summary (Multiple Arguments)
-- Inserts a record into monthly_summary with specified values
INSERT INTO monthly_summary (month, day, vehicle_count, max_dba)
VALUES (%s, %s, %s, %s);

-- 18. Insert into daily_summary (Multiple Arguments)
-- Inserts a record into daily_summary with specified values
INSERT INTO daily_summary (date, hour, ten_min_interval, vehicle_count, max_dba)
VALUES (%s, %s, %s, %s, %s);

-- 19. Create index on TrafficData.dto (No Arguments)
-- Creates an index on the dto column for faster queries
CREATE INDEX idx_dto ON TrafficData (dto);

-- 20. Drop TrafficData table (No Arguments)
-- Drops the TrafficData table if it exists
DROP TABLE IF EXISTS TrafficData;

-- 21. Drop AudioData table (No Arguments)
-- Drops the AudioData table if it exists
DROP TABLE IF EXISTS AudioData;

-- 22. Drop monthly_summary table (No Arguments)
-- Drops the monthly_summary table if it exists
DROP TABLE IF EXISTS monthly_summary;

-- 23. Drop daily_summary table (No Arguments)
-- Drops the daily_summary table if it exists
DROP TABLE IF EXISTS daily_summary;

-- 24. Vehicle details by traffic_id (traffic_id: INT)
-- Returns traffic_id, dto, max_dba, and debug_img for the specified traffic_id
SELECT a.traffic_id, t.dto, a.max_dba, t.debug_img
FROM AudioData a
JOIN TrafficData t ON a.traffic_id = t.traffic_id
WHERE a.traffic_id = %s;

-- 25. Top 5 high dBA values and details (No Arguments)
-- Returns traffic_id, dto, max_dba, and debug_img for the top 5 records by max_dba
SELECT a.traffic_id, t.dto, a.max_dba, t.debug_img
FROM AudioData a
JOIN TrafficData t ON a.traffic_id = t.traffic_id
ORDER BY a.max_dba DESC
LIMIT 5;