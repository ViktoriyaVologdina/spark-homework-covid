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

# 1. Топ-15 стран с наибольшим процентом переболевших на 31 марта 2021
task_1 = countries \
    .filter(col("date") == "2021-03-31") \
    .withColumn("percent_cases", round(col("total_cases") / col("population") * 100, 2)) \
    .select(
        col("iso_code"),
        col("location").alias("country"),
        col("percent_cases")
    ) \
    .orderBy(desc("percent_cases")) \
    .limit(15)

print("Задание 1. Топ-15 стран по проценту переболевших на 31 марта 2021")
task_1.show(15, truncate=False)

rows = task_1.collect()

with open("result_task_1.txt", "w", encoding="utf-8") as file:
    file.write("iso_code,country,percent_cases\n")

    for row in rows:
        file.write(f"{row['iso_code']},{row['country']},{row['percent_cases']}\n")

spark.stop()