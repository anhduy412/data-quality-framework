# cần 1 query để return ra tất cả những result mong muốn (metric value, row count, timestamp) -> check kết quả với giá trị quá khứ
# step 0: kéo dữ liệu quá khứ -> lưu vào 1 bảng
# step 1: query các result mong muốn với dữ liệu hiện tại 
# step 2: so sánh các result của dữ liệu hiện tại với dữ liệu quá khứ, pass/fail -> lưu vào bảng
# step 3: gửi thông báo qua telegram
# viết script generate 1 file config

from pyspark.sql import SparkSession, functions as F
import subprocess
import time
import sys
import os
import config 
from pyspark.sql.functions import col, lit, when, current_timestamp



timestamp_column = config.timestamp_column  # Replace with the actual timestamp column name
cutoff_time = "12:00:00"  # Cutoff time for data readiness
frequency = config.freq

# Initialize Spark session
spark = SparkSession.builder.appName("Data Quality Check").getOrCreate()

def send_msg_to_telegram(msg):
    url = f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN ']} /sendMessage?chat_id={os.environ['TELEGRAM_CHAT_ID']}&text={msg}"
    return subprocess.run(['curl', '-x', url])

# def read_data_from_singlestore():
#     

def count_rows(df):
    return df.count()

# Step 0: Load historical data
def load_historical_data():
    return spark.sql(
        """
        SELECT metric_name, metric_value 
        FROM historical_metrics
    """
    )



# Step 1: Query current metrics
def query_current_metrics():
    return spark.sql(
        """
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
    """
    )

# Step 2: Compare current metrics with historical metrics
def compare_metrics(historical_data, current_data):
    return (
        broadcast(historical_data)
        .alias("h")
        .join(
            current_data.alias("c"),
            col("h.metric_name") == col("c.metric_name"),
            "outer",
        )
        .select(
            col("c.metric_name").alias("metric_name"),
            col("c.metric_value").alias("current_value"),
            col("h.metric_value").alias("historical_value"),
            when(col("c.metric_value") == col("h.metric_value"), lit("PASS"))
            .otherwise(lit("FAIL"))
            .alias("status"),
            current_timestamp().alias("comparison_timestamp"),
        )
    )

# Main logic
if __name__ == "__main__":
    # Step 0: Load historical data
    historical_data = load_historical_data()
    
    # Step 1: Query current metrics
    current_data = query_current_metrics()
    
    # Step 2: Compare and save results
    comparison_result = compare_metrics(historical_data, current_data)
    
    # Lưu kết quả so sánh vào bảng mới dưới dạng Parquet (tối ưu lưu trữ)
    comparison_result.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("comparison_results")
    
    print("Optimized comparison completed and results saved to 'comparison_results' table.")