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

# 2. Топ-10 стран с максимальным количеством новых случаев за последнюю неделю марта 2021
week_df = countries.filter(
    (col("date") >= "2021-03-25") &
    (col("date") <= "2021-03-31")
)

# Для каждой страны ищем день, когда было максимальное количество новых случаев
window_max_cases = Window.partitionBy("location").orderBy(desc("new_cases"))

task_2 = week_df \
    .withColumn("rn", row_number().over(window_max_cases)) \
    .filter(col("rn") == 1) \
    .select(
        col("date"),
        col("location").alias("country"),
        col("new_cases")
    ) \
    .orderBy(desc("new_cases")) \
    .limit(10)

print("Задание 2. Топ-10 стран по максимальному числу новых случаев за последнюю неделю марта 2021")
task_2.show(10, truncate=False)

rows = task_2.collect()

with open("result_task_2.txt", "w", encoding="utf-8") as file:
    file.write("iso_code,country,percent_cases\n")

    for row in rows:
        file.write(f"{row['iso_code']},{row['country']},{row['percent_cases']}\n")

spark.stop()