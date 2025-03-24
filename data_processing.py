# data_processing.py
from pyspark.sql import SparkSession
import json

# Initialize Spark session
spark = SparkSession.builder.appName("people").getOrCreate()

# Read the dataset
print("Reading dataset.csv...")
path_people = "people.csv"
df_people = spark.read.csv(path_people, header=True, inferSchema=True)

# Process the data: Rename columns
df_people = df_people.withColumnRenamed("date of birth", "birth")

# Create a temporary view for SQL querying
df_people.createOrReplaceTempView("people")

# Query 1: Describe the table
query = 'DESCRIBE people'
spark.sql(query).show(20)

# Query 2: Get people with 'male' sex, ordered by birth
query = """SELECT name, birth FROM people WHERE sex='male' ORDER BY birth"""
df_people_names = spark.sql(query)
df_people_names.show(20)

# Query 3: Filter people born between 1903-01-01 and 1950-12-31
query = 'SELECT name, birth FROM people WHERE birth BETWEEN "1903-01-01" AND "1950-12-31" ORDER BY birth'
df_people_1903_1906 = spark.sql(query)
df_people_1903_1906.show(20)

# Convert to JSON format and write to file
results = df_people_1903_1906.toJSON().collect()

# Write processed data to JSON
output_path = "results/data.json"
with open(output_path, 'w') as file:
    json.dump(results, file)

# Query 4: Group by sex for people born between 1903-01-01 and 1911-12-31
query = 'SELECT sex, COUNT(sex) FROM people WHERE birth BETWEEN "1903-01-01" AND "1911-12-31" GROUP BY sex'
df_people_1903_1906_sex = spark.sql(query)
df_people_1903_1906_sex.show()

# Stop Spark session
spark.stop()
