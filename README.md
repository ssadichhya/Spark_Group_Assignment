# Airbnb Boston Data Analysis

This project aims to analyze the Boston Airbnb dataset using PySpark. The dataset contains three main dataframes: listings_df, calendar_df, and reviews_df. Each dataframe provides valuable information about Airbnb listings, their availability, and guest reviews in Boston.

## Prerequisites

Before running the PySpark code, ensure that you have the following prerequisites installed:

- Apache Spark
- PySpark
- Jupyter Notebook (optional)

You can install PySpark and set up your environment using the official [PySpark documentation](https://spark.apache.org/docs/latest/api/python/getting_started/index.html).


## Data Description

The dataset consists of the following dataframes:

1. listings_df: Contains information about Airbnb listings in Boston.
2. calendar_df: Provides details about the availability and pricing of listings over time.
3. reviews_df: Contains guest reviews for listings.
## Questions and Tasks

### Task 1 - User-Defined Function (UDF)

*Question 1:* To create a User-Defined Function (UDF) that converts extracted months from integers to their corresponding month names.

### Task 2 - Booking Analysis

*Question 2:* Busiest Booking Month: Using the date column in the calendar_df, determine which month in Boston had the highest number of bookings. We can use aggregation functions like groupBy and count.

### Task 3 - Vacancy Analysis

*Question 3:* Calculate the number of days since the last booking for each listing in Boston. Use a Window Function to find listings that have been vacant for the longest period.

### Task 4 - Host Analysis

*Question 4:* Find the top 5 hosts in Boston with the highest average nightly price for their listings, along with the number of reviews they've received and the average number of bathrooms for their listings.

### Task 5 - Data Pivot

*Question 5:* Pivot the data to create a table that shows the count of superhosts and non-superhosts for each property type in Boston. Rows represent property types, and columns represent counts of superhosts and non-superhosts.

### Task 6 - UDF for Price Range

*Question 6:* Create a UDF to categorize listings in Boston based on their price range (e.g., budget, mid-range, luxury). Apply this UDF to the listings_df dataframe to categorize the listings.

### Task 7 - Review Analysis

*Question 7:* Combine reviews_df and listings_df to study the number of reviews for each property type in Boston. Order the results by property type.

### Task 8 - Seasonal Price Change

*Question 8:* Identify listings with the highest seasonal price changes by comparing prices in different months.

### Task 9 - Room and Property Analysis

*Question 9:* Group listings by room_type and property_type, calculate the average price for each combination, and rename the resulting column as "Average Price." Order the results by the average price in ascending order.

### Task 10 - Data Pivot (Part 2)

*Question 10:* In this question, you'll need to use the pivot operation to transform the data in such a way that the resulting table displays property types as rows and room types as columns. The values in the table should represent the count of Airbnb listings in Boston for each combination of property type and room type.

### Task 11 - Sentiment Analysis

*Question 11:* Create a UDF to perform sentiment analysis on the review comments and categorize them as positive, neutral, or negative. Apply this UDF to reviews_df to analyze the sentiment of reviews.

### Task 12 - Monthly Review Analysis

*Question 12:* Using groupBy, calculate the total number of reviews left for Boston listings each month. Rename the resulting columns to "Month" and "Total Reviews."

### Task 13 - Popularity Analysis

*Question 13:* Identify significant popularity changes in Airbnb listings in Boston based on month-to-month booking.

### Task 14 - Booking Count by Property Type

*Question 14:* You are given two DataFrames: listings_df and calendar_df, containing information about Airbnb listings and their availability in Boston over time. Your task is to analyze the demand for different property types in Boston by calculating the count of bookings for each property type.

### Task 15 - Property Count Analysis

*Question 15:* How many properties are there in each category that have more or less than 50 reviews.

## Running the Code

To run the PySpark code and explore the analysis, you can use the provided Jupyter Notebook files in the notebooks/ directory. Make sure to adjust the file paths according to your local setup.

## Data Source

The Boston Airbnb dataset used in this project is sourced from [Inside Airbnb](https://www.kaggle.com/datasets/airbnb/boston?select=calendar.csv).

## Acknowledgments

- Airbnb for providing the dataset.
- Apache Spark and PySpark community for powerful data analysis tools.
