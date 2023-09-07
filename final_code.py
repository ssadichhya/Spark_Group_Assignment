from pyspark.sql import SparkSession,functions as F
from pyspark.sql.functions import when, col, avg,udf, month,max, datediff, lit, to_date,count, rank, date_format,lag, when, isnull, abs
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DateType
from textblob import TextBlob


# Initialize a Spark session
spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

# **Reading csv**

#reading csv file
listing_df = spark.read.csv('cleaned_data/clean_listing_csv/', header=True, inferSchema=True)
review_df = spark.read.csv('cleaned_data/clean_reviews_csv/', header=True, inferSchema=True)
calender_df = spark.read.csv('cleaned_data/clean0_calendar_csv/', header=True, inferSchema=True)


# Question 1: To create a User-Defined Function (UDF) that converts extracted months from integers to their corresponding month names

# Define a UDF to convert month integers to month names
def int_to_month_name(month_int):
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    return month_names[month_int - 1] if 1 <= month_int <= 12 else None

# Register the UDF with Spark
month_name_udf = udf(int_to_month_name, StringType())

# Apply the UDF to the DataFrame to create a new column
df_with_month_name = calender_df.withColumn("month_name", month_name_udf(month(col('date'))))

# Show the result
df_with_month_name.show()


# Question 2 - Busiest Booking Month:
# Using the date column in the calendar_df, determine which month in Boston had the highest number of bookings. We can use aggregation functions like groupBy and count.

# Filter for Boston listings (you can adjust the city name as needed)
boston_calendar = calender_df.join(listing_df, calender_df.listing_id == listing_df.id, "inner")

# Extract the month from the 'date' column
boston_calendar = boston_calendar.withColumn('booking_month', month_name_udf(month(col('date'))))
# Filter the DataFrame to include only rows where "available" is False
booked_calendar = boston_calendar.filter(col('available') == 'False')

# Group by the booking month and count the number of bookings
booking_counts = booked_calendar.groupBy('booking_month').count()

# Find the month with the highest number of bookings
busiest_month = booking_counts.orderBy(col('count').desc()).first()

# Show the result
print(f"The busiest booking month in Boston is month {busiest_month['booking_month']} with {busiest_month['count']} bookings.")

# Question 3: Calculate the number of days since the last booking for each listing in Boston. Use a Window Function to find listings that have been vacant for the longest period.

# Filter calendar_df to include only Boston listings
boston_calendar = calender_df.join(listing_df.select('id'), calender_df.listing_id == listing_df.id, "inner")
booked_calendar = boston_calendar.filter(col('available') == 'False')

# # Calculate the maximum date for each listing
window_spec = Window.partitionBy('listing_id').orderBy(col('date').desc())
max_date_df = booked_calendar.select('listing_id', 'date').withColumn('max_date', max('date').over(window_spec))

# min_date_df = max_date_df.select('listing_id', 'date','max_date').withColumn('min_date', min('date').over(window_spec))


# # # Get the maximum date as a separate variable
# max_date = max_date_df.select(max_date_df['max_date']).first()[0]

# # Calculate the number of days since the last booking
vacant_listings = max_date_df.withColumn('days_since_last_booking', datediff(col('max_date'), col('date')))

# # Show the result
vacant_listings.show()

# Question 4- find the top 5 hosts in Boston with the highest average nightly price for their listings, along with the number of reviews they've received and the average number of bathrooms for their listings,

# Filter listings for Boston
boston_listings = listing_df.filter(col('city') == 'Boston')

# Calculate the average nightly price for each listing and the total number of reviews for each host
listing_avg_price_reviews = boston_listings.groupBy('host_name').agg(
    avg(col('price').cast("float")).alias('avg_nightly_price'),
    count('number_of_reviews').alias('total_reviews'),
    avg(col('bathrooms')).alias('avg_bathrooms')  # Calculate the average number of bathrooms
)

# Use window functions to rank hosts by average nightly price
window_spec = Window.orderBy(col('avg_nightly_price').desc())
ranked_hosts = listing_avg_price_reviews.withColumn('rank', rank().over(window_spec))

# Filter and select the top 5 hosts with the highest average nightly price
top_5_hosts = ranked_hosts.filter(col('rank') <= 5).select('host_name', 'avg_nightly_price', 'total_reviews', 'avg_bathrooms')

# Show the result
top_5_hosts.show()

# Question 5 - Superhosts by Property Type:
# Pivot the data to create a table that shows the count of superhosts and non-superhosts for each property type in Boston. Rows represent property types, and columns represent counts of superhosts and non-superhosts.

# Filter listings for Boston
boston_listings = listing_df.filter(col('city') == 'Boston')

# Pivot the data to count superhosts and non-superhosts for each property type
pivot_table = boston_listings.groupBy('property_type') \
    .pivot('host_is_superhost', ['True', 'False']) \
    .count() \
    .fillna(0)

# Rename the columns for clarity
pivot_table = pivot_table.withColumnRenamed('True', 'Superhost Count') \
    .withColumnRenamed('False', 'Non-Superhost Count')

# Show the result
pivot_table.show()


# Question 6: Create a UDF to categorize listings in Boston based on their price range (e.g., budget, mid-range, luxury). Apply this UDF to the listings_df dataframe to categorize the listings.

# Define the UDF to categorize listings based on their price
def categorize_price_range(price):
    if price < 5000:
        return "Budget"
    elif price >= 5000 and price < 10000:
        return "Mid-Range"
    else:
        return "Luxury"

# Register the UDF with Spark
categorize_price_udf = udf(categorize_price_range, StringType())

# Apply the UDF to categorize listings
listing_df_q1 = listing_df.withColumn("price_range_category", categorize_price_udf(col("price").cast("float")))

# Show the resulting DataFrame with the new category column
listing_df_q1.select("id", "price", "price_range_category").show()




# Question 7: Combine reviews_df and listings_df to study the number of reviews for each property type in Boston. Order the results by property type.

join_expression = review_df["listing_id"] == listing_df["id"]


#  Join reviews_df with listings_df on "listing_id"
combined_df = review_df.join(listing_df, join_expression, "inner")

# Group by "property_type" and count the number of reviews for each property type
review_count_by_property_type = combined_df.groupBy("property_type") \
    .count() \
    .withColumnRenamed("count", "ReviewCount")

# Order the results by property type
review_count_by_property_type = review_count_by_property_type.orderBy(col("ReviewCount").desc())

# Show the resulting DataFrame
review_count_by_property_type.show()

# Question 8: Identify listings with the highest seasonal price changes by comparing prices in different months.



# Define a window specification to partition by listing_id and order by month
window_spec = Window.partitionBy("listing_id").orderBy(F.year("date"), F.month("date"))

# Calculate the average price for each listing in each month
avg_price_by_month = calender_df.withColumn("avg_price", F.avg("price").over(window_spec))

# Calculate the price change for each listing in each month compared to the previous month
price_change = avg_price_by_month.withColumn("price_change", F.col("avg_price") - F.lag("avg_price").over(window_spec))

# Find the maximum price change (seasonal price change) for each listing
seasonal_price_change = price_change.groupBy("listing_id").agg(F.max("price_change").alias("max_seasonal_price_change"))

# Rank the listings based on their seasonal price change
ranked_listings = seasonal_price_change.orderBy(F.desc("max_seasonal_price_change"))

# Show the resulting DataFrame with the ranked listings
ranked_listings.show()

# Question 9:Group listings by room_type and property_type, calculate the average price for each combination, and rename the resulting column as "Average Price." Order the results by the average price in ascending order.

listing_df_q = listing_df.withColumnRenamed("room_type", "Room Type")
listing_df_q = listing_df_q.withColumnRenamed("property_type", "Property Type")
listing_df_q = listing_df_q.withColumnRenamed("price", "Price")

# Calculate the average price for each combination of room type and property type
average_price_df = listing_df_q.groupBy("Room Type", "Property Type") \
    .agg(avg("Price").alias("Average Price"))

# Order the results by "Average Price" in ascending order
ordered_average_price_df = average_price_df.orderBy(col("Average Price"))

# Show the resulting DataFrame with the renamed column
ordered_average_price_df.show()


# Question 10: In this question, you'll need to use the pivot operation to transform the data in such a way that the resulting table displays property types as rows and room types as columns. The values in the table should represent the count of Airbnb listings in Boston for each combination of property type and room type.

# Filter the data to include only Boston listings
boston_listings = listing_df.filter(col('city') == 'Boston')

# Group the data by property type and room type to calculate counts
grouped_data = boston_listings.groupBy('property_type', 'room_type') \
    .agg(F.count("*").alias("listing_count"))

# Pivot the data to have property types as rows, room types as columns, and listing counts as values
pivoted_table = grouped_data.groupBy('property_type') \
    .pivot('room_type') \
    .agg(F.sum('listing_count').alias('total_listings')) \
    .fillna(0)

# Show the resulting table
pivoted_table.show()

# Question 11: UDF Problem: Create a UDF to perform sentiment analysis on the review comments and categorize them as positive, neutral, or negative. Apply this UDF to reviews_df to analyze the sentiment of reviews.


# Define a function to perform sentiment analysis using TextBlob
def analyze_sentiment(comment):
    analysis = TextBlob(comment)
    # Classify sentiment as positive, neutral, or negative based on polarity
    if analysis.sentiment.polarity > 0:
        return "positive"
    elif analysis.sentiment.polarity == 0:
        return "neutral"
    else:
        return "negative"

# Register the UDF
sentiment_analysis_udf = udf(analyze_sentiment, StringType())

reviews_sentiment = review_df.withColumn("sentiment", sentiment_analysis_udf(col("comments")))

reviews_sentiment.show(50, truncate=False)

# Question 12: Using groupBy, calculate the total number of reviews left for Boston listings each month. Rename the resulting columns to "Month" and "Total Reviews."



# Assuming 'date_string' is in the format 'yyyy-MM-dd'
date_format = 'yyyy-MM-dd'

# Use the to_date function to convert the string column to a date column
reviews_clean_1 = review_df.withColumn("date", to_date(review_df["date"], date_format).cast(DateType()))


reviews_U2 = reviews_clean_1.withColumn("month", month(reviews_clean_1["date"]))
#reviews_U2 = reviews_U2.withColumn("month_name", date_format(reviews_U2["month"], "MMM"))

# Group by Month, and count the number of reviews
monthly_reviews = reviews_U2.groupBy("Month") \
    .count() \
    .withColumnRenamed("count", "Total Reviews") \
    .orderBy("Month")

# Show the resulting DataFrame
monthly_reviews.show()

# Question 13: Identify significant popularity changes in Airbnb listings in Boston based on month-to-month booking. 

calendar_window = calender_df.withColumn("date", to_date(calender_df["date"]))

# Calculate the total number of bookings per listing
bookings_count = calendar_window.filter(calender_df["available"] == False) \
    .groupBy("listing_id") \
    .agg(count("listing_id").alias("total_bookings"))

# Calculate the number of bookings per listing for each month
bookings_count = calendar_window.filter(calendar_window["available"] == False) \
    .groupBy("listing_id", month("date").alias("month")) \
    .agg(count("listing_id").alias("bookings"))

bookings_count.show()

window_spec = Window.partitionBy("listing_id").orderBy("month")
monthly_bookings = bookings_count.withColumn("previous_month_bookings", lag("bookings").over(window_spec))

monthly_bookings1 = monthly_bookings.withColumn("popularity_change", when(isnull(monthly_bookings["previous_month_bookings"]), 0)
                                                .otherwise(monthly_bookings["bookings"] - monthly_bookings["previous_month_bookings"]))


significant_popularity_changes = monthly_bookings1.filter(abs("popularity_change") >= 0.5)
significant_popularity_changes.show()

# Question 14: You are given two DataFrames: listings_df and calendar_df, containing information about Airbnb listings and their availability in Boston over time. Your task is to analyze the demand for different property types in Boston by calculating the count of bookings for each property type. 

calendar_cleanp = calender_df.filter(calender_df["available"]==False)

# calendar_cleanp.show()

property_demand = listing_df.join(calendar_cleanp, listing_df["id"] == calendar_cleanp["listing_id"]) \
    .groupBy("property_type") \
    .agg(count("listing_id").alias("booking_count"))

property_demand = property_demand.withColumn("Demand_Category",
                                             when(property_demand["booking_count"] > 1000, "High Demand")
                                             .otherwise("Low Demand"))
                                             
property_demand.show()

pivot_table = property_demand.groupBy("property_type") \
    .pivot("Demand_Category", ["High Demand", "Low Demand"]) \
    .agg(count("property_type")) \
    .withColumnRenamed("High Demand", "IS_High_Demand") \
    .withColumnRenamed("Low Demand", "IS_Low_Demand")


pivot_table = pivot_table.withColumn("IS_High_Demand", when(col("IS_High_Demand") > 0, True).otherwise(False))
pivot_table = pivot_table.withColumn("IS_Low_Demand", when(col("IS_Low_Demand") > 0, True).otherwise(False))


pivot_table.show()

# QUestion 15: How many property are there in each category that have more or less than 50 reviews

reviews_count = review_df.groupBy("listing_id").agg(count("*").alias("total_reviews_count"))

reviews_count.show()


