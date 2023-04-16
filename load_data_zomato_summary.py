import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, when, lit, isnan, to_timestamp, row_number, desc, udf, lower as df_lower
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

def load_data_to_zomato_summary(file_date, country, application_name, database_name, user_id, create_datetime, indian_cusines):
    spark = SparkSession.builder \
        .master('yarn') \
        .appName(application_name) \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .config('hive.metastore.uris', 'thrift://localhost:9083') \
        .enableHiveSupport() \
        .getOrCreate()

    data_query="SELECT * FROM "+database_name+".zomato"
    if file_date != "None":
        data_query += " where file_date = '"+file_date+"'"

    list_indian_cuisines = list(map(lambda x: x.lower(), indian_cusines.split(",")))

    @udf(returnType=StringType())
    def compare_cuisines(cuisines):
        for indian_cuisine in list_indian_cuisines:
            if indian_cuisine in cuisines:
                return "Indian"
        return "World Cuisines"

    windowSpec = Window.partitionBy("restaurant_id").orderBy(desc("timestamp"))
    country_code_df = spark.sql("SELECT * FROM "+database_name+".dim_country")
   
    zomato_summary_df = spark.sql(data_query) \
            .withColumn("m_rating_colour", 
                   when((col("aggregate_rating") >= 1.9) & (col("aggregate_rating") <= 2.4), "Red")
                  .when((col("aggregate_rating") >= 2.5) & (col("aggregate_rating") <= 3.4), "Amber")
                  .when((col("aggregate_rating") >= 3.5) & (col("aggregate_rating") <= 3.9), "Light Green")
                  .when((col("aggregate_rating") >= 4.0) & (col("aggregate_rating") <= 4.4), "Green")
                  .when((col("aggregate_rating") >= 4.5) & (col("aggregate_rating") <= 5), "Gold")
                  .otherwise("NA")) \
            .withColumn("m_cuisines", compare_cuisines(df_lower(col("cuisines")))) \
            .withColumnRenamed("file_date", "p_filedate") \
            .join(country_code_df, "country_code", "left_outer") \
            .withColumnRenamed("country", "p_country_name") \
            .withColumn("create_datetime", lit(create_datetime)) \
            .withColumn("user_id", lit(user_id)) \
            .filter((col("cuisines").isNotNull()) & ~(isnan(col("cuisines"))) & (col("cuisines") != "")) \
            .withColumn("timestamp", to_timestamp(col("p_filedate"), "yyyyMMdd_HHmmss")) \
            .withColumn("rank", row_number().over(windowSpec)) \
            .filter(col("rank") == 1) \
            .select("restaurant_id","restaurant_name","country_code","city","address","locality","locality_verbose",
            "longitude","latitude","cuisines","average_cost_for_two","currency","has_table_booking","has_online_delivery",
            "is_delivering_now","switch_to_order_menu","price_range","aggregate_rating","rating_text","votes","m_rating_colour",
            "m_cuisines","create_datetime","user_id","p_filedate","p_country_name")
    
    string_columns = [column[0] for column in zomato_summary_df.dtypes if column[1] == "string"]
    zomato_summary_df = zomato_summary_df.na.fill("NA",string_columns)
    
    if country != "None":
        zomato_summary_df = zomato_summary_df.filter(col("country") == country)

    zomato_summary_df.write.format("orc").mode("overwrite").insertInto(database_name+".zomato_summary")
    
    
   
if __name__ == '__main__':
    application_name = sys.argv[1]
    database_name = sys.argv[2]
    user_id = sys.argv[3]
    create_datetime = sys.argv[4]
    indian_cusines = sys.argv[5]
    file_date = sys.argv[6]
    country_code = sys.argv[7]
    load_data_to_zomato_summary(file_date, country_code, application_name, database_name, user_id, create_datetime, indian_cusines)
