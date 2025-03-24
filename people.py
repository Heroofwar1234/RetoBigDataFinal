from pyspark.sql import SparkSession
import json
import os

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder.appName("People Data Processing").getOrCreate()

    # Define dataset path
    path_people = "people.csv"
    
    print(f"Reading dataset from {path_people} ...")
    
    # Load dataset
    df_people = spark.read.csv(path_people, header=True, inferSchema=True)
    df_people = df_people.withColumnRenamed("date of birth", "birth")

    # Create SQL view for queries
    df_people.createOrReplaceTempView("people")

    # Describe table structure
    spark.sql("DESCRIBE people").show()

    # Select males ordered by birth date
    query_males = "SELECT name, birth FROM people WHERE sex = 'male' ORDER BY birth"
    df_males = spark.sql(query_males)
    df_males.show()

    # Select people born between 1903 and 1950
    query_1903_1950 = """
        SELECT name, birth FROM people 
        WHERE birth BETWEEN '1903-01-01' AND '1950-12-31' 
        ORDER BY birth
    """
    df_1903_1950 = spark.sql(query_1903_1950)
    df_1903_1950.show()

    # Ensure results directory exists
    results_dir = "results"
    os.makedirs(results_dir, exist_ok=True)

    # Convert to JSON and write to file
    results_json = df_1903_1950.toJSON().collect()
    json_output_path = os.path.join(results_dir, "data.json")
    
    with open(json_output_path, 'w') as file:
        json.dump(results_json, file, indent=4)

    print(f"Data saved to {json_output_path}")

    # Group by sex for people born between 1903 and 1911
    query_sex_count = """
        SELECT sex, COUNT(sex) AS count 
        FROM people 
        WHERE birth BETWEEN '1903-01-01' AND '1911-12-31' 
        GROUP BY sex
    """
    df_sex_count = spark.sql(query_sex_count)
    df_sex_count.show()

    # Stop Spark session
    spark.stop()
