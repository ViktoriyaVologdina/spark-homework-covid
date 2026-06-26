from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, desc, row_number, lag
from pyspark.sql.window import Window

# Создаем Spark-сессию
spark = SparkSession.builder \
    .appName("CovidSparkSQLHomework") \
    .master("local[*]") \
    .getOrCreate()

# Загружаем датасет
df = spark.read.csv("covid-data.csv", header=True, inferSchema=True)

# Убираем агрегированные строки OWID, оставляем только отдельные страны
countries = df.filter(~col("iso_code").startswith("OWID"))

# 3. Изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021
russia_df = countries.filter(col("location") == "Russia")

# Окно по датам для получения значения предыдущего дня
window_russia = Window.orderBy("date")

task_3 = russia_df \
    .withColumn("yesterday_cases", lag("new_cases").over(window_russia)) \
    .filter(
        (col("date") >= "2021-03-25") &
        (col("date") <= "2021-03-31")
    ) \
    .withColumn("delta", col("new_cases") - col("yesterday_cases")) \
    .select(
        col("date"),
        col("yesterday_cases"),
        col("new_cases").alias("today_cases"),
        col("delta")
    ) \
    .orderBy("date")

print("Задание 3. Изменение новых случаев в России относительно предыдущего дня")
task_3.show(20, truncate=False)

rows = task_3.collect()

with open("result_task_3.txt", "w", encoding="utf-8") as file:
    file.write("date,yesterday_cases,today_cases,delta\n")

    for row in rows:
        file.write(
            f"{row['date']},{row['yesterday_cases']},{row['today_cases']},{row['delta']}\n"
        )

spark.stop()