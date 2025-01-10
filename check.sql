-- Tạo bảng lưu dữ liệu quá khứ nếu chưa có
CREATE TABLE IF NOT EXISTS historical_data (
    name VARCHAR(255),
    date DATE,
    row_count INT,
    metric VARCHAR(255),
    metric_value FLOAT,
    threshold FLOAT
);

-- get data from the past
INSERT INTO historical_data (name, date, row_count, metric, metric_value, threshold)
SELECT 
    'ExampleName' AS name,
    CURRENT_DATE - INTERVAL '1 DAY' AS date,
    COUNT(*) AS row_count,
    'ExampleMetric' AS metric,
    AVG(column_name) AS metric_value,
    NULL AS threshold -- Threshold sẽ được cập nhật sau
FROM 
    your_table
WHERE 
    some_condition
GROUP BY 
    'ExampleName', CURRENT_DATE - INTERVAL '1 DAY';

-- Query dữ liệu hiện tại
WITH current_data AS (
    SELECT 
        'ExampleName' AS name,
        CURRENT_DATE AS date,
        COUNT(*) AS row_count,
        'ExampleMetric' AS metric,
        AVG(column_name) AS metric_value
    FROM 
        your_table
    WHERE 
        some_condition
    GROUP BY 
        'ExampleName', CURRENT_DATE
)

-- calculate threshold
WITH avg_past_row_count AS (
    SELECT 
        name,
        AVG(row_count) AS avg_row_count_3_days
    FROM 
        historical_data
    WHERE 
        date >= CURRENT_DATE - INTERVAL '4 DAY' AND date < CURRENT_DATE
    GROUP BY 
        name
),
current_data_with_threshold AS (
    SELECT 
        c.name,
        c.date,
        c.row_count,
        c.metric,
        c.metric_value,
        (c.row_count * 100.0 / COALESCE(p.avg_row_count_3_days, 1)) AS threshold -- Tính threshold (%)
    FROM 
        current_data c
    LEFT JOIN 
        avg_past_row_count p
    ON 
        c.name = p.name
)

-- create table to save comparison results if not exists
CREATE TABLE IF NOT EXISTS comparison_results (
    name VARCHAR(255),
    date DATE,
    row_count INT,
    metric VARCHAR(255),
    metric_value FLOAT,
    threshold FLOAT,
    pass_metric BOOLEAN
);

-- compare and save
INSERT INTO comparison_results (name, date, row_count, metric, metric_value, threshold, pass_metric)
SELECT 
    c.name,
    c.date,
    c.row_count,
    c.metric,
    c.metric_value,
    c.threshold,
    CASE 
        WHEN c.threshold >= 90 AND c.threshold <= 110 THEN TRUE -- Pass nếu trong ngưỡng 90%-110%
        ELSE FALSE
    END AS pass_metric
FROM 
    current_data_with_threshold c;