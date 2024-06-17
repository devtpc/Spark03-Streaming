# Spark Streaming Homework

## Introduction

This project is a homework at the EPAM Data Engineering Mentor program. The main idea behind this task is to get a deeper understanding of streaming processes and how to work with them within the Databricks platform. The original copyright belongs to [EPAM](https://www.epam.com/). 

#### Some original instructions about the task:

* Download/fork the backbone project
* Download the [data files](https://static.cdn.epam.com/uploads/583f9e4a37492715074c531dbd5abad2/dse/m13sparkstreaming.zip). Unzip it and upload into provisioned via Terraform storage account.
* Organize incremental copy of hotel/weather data from local into provisioned with terraform Storage Account (with a delay, one day per cycle).
* Create Databricks Notebooks (storage libraries are already part of Databricks environment).
* Create Spark Structured Streaming application with Auto Loader to incrementally and efficiently processes hotel/weather data as it arrives in provisioned Storage Account. Using Spark calculate in Databricks Notebooks for each city each day:
  * Number of distinct hotels in the city.
  * Average/max/min temperature in the city.
* Visualize incoming data in Databricks Notebook for 10 biggest cities (the biggest number of hotels in the city, one chart for one city)
  * X-axis: date (date of observation)
  * Y-axis: number of distinct hotels, average/max/min temperature.
* Deploy Databricks Notebook on cluster, to setup infrastructure use terraform scripts from module. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.

## About the repo

This repo is hosted [here](https://github.com/devtpc/Spark03-Streaming)

> [!NOTE]
> The original data files are not included in this repo, only the link.
> Some sensitive files, like configuration files, API keys, tfvars are not included in this repo.

## Documentation


Setting up the infrastructure is very similar to one of my previous works, [Spark SQL](https://github.com/devtpc/Spark02-SQL) Although I document the similar parts as well, they are not as detailed. In these cases please refer to [there](https://github.com/devtpc/Spark02-SQL) for detailed explanations, especially the detailed commands behind the 'Makefile' commands 

## Prerequisites

* The necessiary software environment should be installed on the computer ( azure cli, terraform, etc.)
* For Windows use Gitbash to run make and shell commands. (Task was deployed and tested on a Windows machine)
* Have an Azure account (free tier is enough)
* Have an Azure storage account ready for hosting the terraform backend


## Preparatory steps

### Download the data files

Download the data files from [here](https://static.cdn.epam.com/uploads/583f9e4a37492715074c531dbd5abad2/dse/m13sparkstreaming.zip).
Exctract the zip file, and copy its content to this repo. Rename the `m13sparkstreaming` folder to `streamdata`, and put it in a folder named `data`.
The file structure should look like this:

![File structure image](/screenshots/img_data_file_structure.png)

### Setup your configuration

Go to the [configcripts folder](/configscripts/) and copy/rename the `config.conf.template` file to `config.conf`. Change the AZURE_BASE, AZURE_LOCATION, and other values as instructed within the file.

In the [configcripts folder](/configscripts/) copy/rename the `terraform_backend.conf.template` file to `terraform_backend.conf`. Fill the parameters with the terraform data.

Propagate your config data to other folders with the [refreshconfs.sh](/configscripts/refresh_confs.sh) script, or with `make refresh-confs` from the main folder

The details are in comments in the config files.

## Creating the Databricks environment

Before starting, make sure, that config files were correctly set up and broadcasted in the 'Setup your configuration'. There should be a `terraform.auto.tvars` file with your azure config settings, and a `backend.conf` file with the backend settings in the terraform folder.

Log in to Azure CLI with `az login`



### Setup the Azure base infrastructure with terraform

Use `make createinfra` command to create your azure infrastructure. To verify the infrastructure visually, login to the Azure portal, and view your resource groups. There are  2 new resource groups:

* the one, which was parameterized, named rg-youruniquename-yourregion, with the Azure Databricks Service and the Storage account.

![Databricks created](/screenshots/img_databricks_created.png)

* Databricks' own resources, starting with databrics-rg-

![Databricks created 2](/screenshots/img_databricks_created_2.png)

After entering the Databricks Service, you can launch the workspace, confirming that it is really working:

![Databricks created 3](/screenshots/img_databricks_created_3.png)

The data container was also created:

![Databricks created 4](/screenshots/img_databricks_created_4.png)

### Save needed keys from Azure
Storage access key will be needed in configuring the cluster, so save the storage account key from Azure by typing `make retrieve-storage-keys`. This saves the storage key to `configscripts/az_secret.conf` file.

### Upload data input files to the storage account

Now, that you have your storage account and key ready, the data files can be uploaded to the storage. Type `make uploaddata` to upload the data files.


![Data upload 1](/screenshots/img_dataupload_1.png)


### Create a Databricks cluster manually

To create the databricks cluster, manually create the cluster at the 'Compute' screen:

![Create Cluster 1](/screenshots/img_create_cluster_1.png)

At the advanced option set the storage account's key at the sparkconfig, and the account name and container name as environment variables, as in the picture below.

![Create Cluster 2](/screenshots/img_create_cluster_2.png)

### Create a Databricks cluster with Terraform


> [!NOTE]
> The cluster can be set up with Terraform as well, however the Azure free Tier's limitations did not make it possibe for me to set it up this way. A more detailed explanation is [here](/texts/limitations/README.md)

## Running the app

Upload [the notebook](/notebooks/StreamingApp.py) as a source to the Azure Databricks portal to your workspace, and run the cells. The notebook should be run cell-by-cell. The executed notebook can be viewed with the result visualizations [here (HTML version)](https://devtpc.github.io/Spark03-Streaming/notebooks/StreamingApp.html) without Databricks.

The notebook is documented in detail with comments, so here I include only the main steps.

### Using Autoloader

The beginning starts with reading the configurations. Then we are configuring the input Dataframe with Autoloader. Loading all the data, without in-built delay is relatively quick. I embedded a delay in the data loading, to simulate real streaming. After some experimenting the option `option("cloudFiles.maxFilesPerTrigger", 50)` was used, this enabled the streaming to run for ~7 minutes

![Autoloader input](/screenshots/img_autoloader_input.png)

### Create aggregated stream

We create the aggregated stream, which can be used for the tasks. As we need hotel count and temperature aggregated data by day and city, we `group by` on `city` and `wthr_date`, and aggreate the hotel count and temperature data.
* Note, that the preliminary exploratory data analysis showed, that there are no duplication of hotels for the same city and day, meaning, that on this aggregation level the distinct hotels and the hotels are the same number, so a simple query is adequate.
* In the input data there are temperature data both in Celsius and Fahrenheit, I choose to work with the Celsius data.
* The temperature input data are already average daily data for the hotels, there are no minimum and maxiumum temperature data within the day. On a city level we can calculate the minimum/average/maxiumum of the averages, however, it means, that in most cases, where the city falls within a single geohash region, the minimum, average and maximum values will be the same. There can be small differences only in case of very big cities, which fall within multiple geohash regions, or which are on the border of two geohash regions.


![Aggregate input](/screenshots/img_aggregate_input.png)

As this aggregated table is the base of our following data, examine the execution plan! This can be done by writing `.explain(extended=True)` after the Dataframe.

![Aggregate input explain ](/screenshots/img_explain_aggr.png)


We can see the [execution plan](/texts/explains/df_aggr/df_aggr_plan.txt), the [executed plan](/texts/explains/df_aggr/df_aggr_result.txt), and the executed [Visual Directed Acyclic Graph](/texts/explains/df_aggr/df_aggr_dag.png)

![Visual Directed Acyclic Graph](/texts/explains/df_aggr/df_aggr_dag.png)

From this graph and the plans we can see, that a single task is a linear task, however, we should note, that as this is part of a streaming, numerous similar tasks are executed.

### Running the stream


We write the aggregated stream to a table in memory. The stream continuously runs, until we stop it. During it's run, we can use the subsequent cells to run our other queries and visualizations. Note, that in a production environment, probably a separate notebook would be used for streaming (to a sink) and analysis.

With the in-built delays, the streaming lasted about 7 minutes, before all records were processed. We can follow along the metrics, as the bathces are running:

![Stream execution metrics 1 img](/screenshots/img_execution_1.png)

![Stream execution metrics 2 img](/screenshots/img_execution_2.png)

![Stream execution metrics 3 img](/screenshots/img_execution_3.png)



### Viewing the results

The following data had to be calculated for each city each day:
  * Number of distinct hotels in the city.
  * Average/max/min temperature in the city.

These data are directly coming from our aggregated table:

![Aggregate table view](/screenshots/img_table_view.png)

### Creating TOP 10 visualizations

Unfortunately, although Databricks has great visualizations, they are not very suitable for creating 10 similar charts for the TOP 10 cities, so  as a workaround, I used `matplotlib.pyplot` with a loop.

The task wasn't very clear on how to interpret the TOP 10 cities by `the biggest number of hotels in the city`, especially, that the input data is very incomplete, and a certain city on one day can have 200 hotels, and on another day only 1 hotel in the database. My approach was to take the number of hotels for a city daily, and use the maximum of the daily numbers as a reference for TOP 10. 
```
top_cities = [row.city for row in spark.sql("SELECT DISTINCT city, MAX(hotels_count) as max_count FROM aggr_table GROUP BY city ORDER BY max_count DESC LIMIT 10").collect()]
```

After selecting the top 10 cities, we can create the visualizations in a loop. In the loop we query the data for the city, convert it to a pandas dataframe, and use plotly to create the plots.
```
    for city in top_cities:
        display_query = f"""
        SELECT city, wthr_date, hotels_count, avg_tmpr_c, min_tmpr_c, max_tmpr_c
        FROM aggr_table
        WHERE city = '{city}'
        ORDER BY wthr_date
        """

        pdf = spark.sql(display_query).toPandas()

```

The temperature date are in the range 10-30, the hotel data can be in the multiple 100s, so on the chart we use dual axis: one axis for the Hotel count, another for the temperatures. This can be achieved by using `ax2 = ax1.twinx()`. The detailed code can be viewed [here](https://devtpc.github.io/Spark03-Streaming/notebooks/StreamingApp.html)


We create the graphs by running `display_subplots()`

Here are the results:

![Results 1 img](/screenshots/img_resultchart_1.png)
![Results 2 img](/screenshots/img_resultchart_2.png)
![Results 3 img](/screenshots/img_resultchart_3.png)
![Results 4 img](/screenshots/img_resultchart_4.png)
![Results 5 img](/screenshots/img_resultchart_5.png)


### Understanding the results

Note, that although we got the expected result, we can observe the high incompleteness, and quality problems of the input data:
* The number of hotels within a city is in a very wide range, from 1 to multiple 100s.
* As there are no real daily minimum and maximum values, the min, max and average temperatures are almost always equal. There are some cases, when they are differ, but only in speciak cases, when there are many hotel data, and the city is big enough, that some parts of it are in different 4-digit geohash locations, or is on a dividing line of multiple geohash locations.



