import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode

def json_to_csv(source_path, destination_path, application_name):
    spark = SparkSession.builder \
        .appName(application_name) \
        .getOrCreate()
    
    df = spark.read.format("json").load(source_path)

    restaurent_df = df.select(explode(col("restaurants")).alias("restaurants")) \
                    .select(
                        col("restaurants.restaurant.id").alias("Restaurant ID"),
                        col("restaurants.restaurant.name").alias("Restaurant Name"),
                        col("restaurants.restaurant.location.country_id").alias("Country Code"),
                        col("restaurants.restaurant.location.city").alias("City"),
                        col("restaurants.restaurant.location.address").alias("Address"),
                        col("restaurants.restaurant.location.locality").alias("Locality"),
                        col("restaurants.restaurant.location.locality_verbose").alias("Locality Verbose"),
                        col("restaurants.restaurant.location.longitude").alias("Longitude"),
                        col("restaurants.restaurant.location.latitude").alias("Latitude"),
                        col("restaurants.restaurant.cuisines").alias("Cuisines"),
                        col("restaurants.restaurant.average_cost_for_two").alias("Average Cost for two"),
                        col("restaurants.restaurant.currency").alias("Currency"),
                        col("restaurants.restaurant.has_table_booking").alias("Has Table booking"),
                        col("restaurants.restaurant.has_online_delivery").alias("Has Online delivery"),
                        col("restaurants.restaurant.is_delivering_now").alias("Is delivering now"),
                        col("restaurants.restaurant.switch_to_order_menu").alias("Switch to order menu"),
                        col("restaurants.restaurant.price_range").alias("Price range"),
                        col("restaurants.restaurant.user_rating.aggregate_rating").alias("Aggregate rating"),
                        col("restaurants.restaurant.user_rating.rating_text").alias("Rating text"),
                        col("restaurants.restaurant.user_rating.votes").alias("Votes")
                    )

    restaurent_df.coalesce(1).write.option("delimiter", "\t").csv(destination_path, header=True)

if __name__ == '__main__':
    source_path = sys.argv[1]
    destination_path = sys.argv[2]
    application_name = sys.argv[3]
    json_to_csv(source_path, destination_path, application_name)
