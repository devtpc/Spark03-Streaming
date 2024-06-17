# Databricks notebook source
import os
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import *
from matplotlib.ticker import MaxNLocator


#env. parameters
STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME")
CONTAINER_NAME = os.environ.get("CONTAINER_NAME")

# custom parameters
INPUT_FOLDER ="streamdata"

# calculated data paths
INPUT_DATA_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{INPUT_FOLDER}"


# COMMAND ----------

# Read Stream with autoloader. 
# Simulate slower streaming by restricting max Files and Bytes per Trigger
schema_location = "/dbfs/testdata/schema"
df_input = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", schema_location)
      .option("cloudFiles.includeExistingFiles", "true")
      .option("cloudFiles.maxFilesPerTrigger", 50)  # Restrict max files per trigger
      .load(INPUT_DATA_PATH))

# COMMAND ----------

# create aggregated dataframe by city and wthr_date
# EDA showed, that there are only distinct data per city and wthr_date for hotels, so we can use it like this

df_aggr = (df_input
           .groupby("city", "wthr_date")
           .agg(
               count("id").alias("hotels_count"),
               avg("avg_tmpr_c").alias("avg_tmpr_c"),
               min("avg_tmpr_c").alias("min_tmpr_c"),
               max("avg_tmpr_c").alias("max_tmpr_c")
           )
           .select("city","wthr_date","hotels_count","avg_tmpr_c","min_tmpr_c","max_tmpr_c")
           .orderBy(["city","wthr_date"])
)

# COMMAND ----------

#Use explain, to examine execution plans
df_aggr.explain(extended=True)

# COMMAND ----------

# Write aggregated data to in-memory stream table 
aggr_table = df_aggr.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("aggr_table") \
    .start()

# COMMAND ----------

# create temp view  for each city each day number of distinct hotels in the city & Average/max/min temperature in the city
spark.sql("CREATE TEMP VIEW aggr_table_view AS SELECT * FROM aggr_table ORDER BY city, wthr_date")

# COMMAND ----------

#Display the Number of distinct hotels in the city and average/max/min temperature in the city.
display(spark.sql("SELECT * FROM aggr_table_view"))


# COMMAND ----------

# Databrick's in-built Visualizations are not suitable for 10 separate subplots
# We will show the 10 subplots using matplotlib

def display_subplots():
    
    #top cities by hotel_count (for the whole data, not just daily)
    top_cities = [row.city for row in spark.sql("SELECT DISTINCT city, MAX(hotels_count) as max_count FROM aggr_table GROUP BY city ORDER BY max_count DESC LIMIT 10").collect()]
   
    #cheate chart for all cities in TOP 10
    for city in top_cities:
        display_query = f"""
        SELECT city, wthr_date, hotels_count, avg_tmpr_c, min_tmpr_c, max_tmpr_c
        FROM aggr_table
        WHERE city = '{city}'
        ORDER BY wthr_date
        """

        # we convert the by city dataframe to pandas df
        pdf = spark.sql(display_query).toPandas()

        # Untomment to Debug: Print data to check correctness
        # print(f"City: {city}")
        # print(pdf)

        #Creating dual-axis plot, ax1: first axis
        fig, ax1 = plt.subplots(figsize=(10, 6))

        # y1 axis: Hotel count, with blue
        ax1.plot(pdf['wthr_date'], pdf['hotels_count'], marker='o', linestyle='-', label='Hotel count')
        ax1.set_ylabel('Hotel count', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')

        #use only integers on the "hotels_count" axis
        ax1.yaxis.set_major_locator(MaxNLocator(integer=True))
        
        #Ensure the y-axis has a proper range even if there's only one unique value
        if pdf['hotels_count'].nunique() == 1:
            single_value = pdf['hotels_count'].iloc[0]
            ax1.set_ylim(single_value - 1, single_value + 1)        

        #rotate x axis labels
        ax1.set_xlabel('Date')
        ax1.tick_params(axis='x', rotation=45)


        #Creating dual-axis plot, ax2: first axis, with temoeratures
        ax2 = ax1.twinx()

        ax2.plot(pdf['wthr_date'], pdf['min_tmpr_c'], color='green', marker='^', linestyle='-', label='Min. temp')
        ax2.plot(pdf['wthr_date'], pdf['max_tmpr_c'], color='orange', marker='d', linestyle='-', label='Max. temp')
        ax2.plot(pdf['wthr_date'], pdf['avg_tmpr_c'], color='red', marker='s', linestyle='-', label='Avg. temp')

        #Needed as a workaround for a bug in very long floating-point numbers        
        ax2.yaxis.set_major_formatter(plt.FormatStrFormatter('%.1f'))

        ax2.set_ylabel('Temperatures', color='black')

        plt.title(f"City: {city}")
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')
        
        plt.legend()
        plt.show()        



# COMMAND ----------

display_subplots()
