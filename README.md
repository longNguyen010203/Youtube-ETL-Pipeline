# 🌄 Youtube-ETL-Pipeline
In this project, I build a simple data pipeline following the ETL(extract - transform - load) model using Youtube-Trending-Video dataset, perform data processing, transformation and calculation using Apache Spark big data technology, serving the video search and recommendation system

## 🔦 About Project
<img src="./public/images/data_flow.png" style="width: 100%;">

 - **Data Source**: This project uses two main `data sources`: [Youtube Trending Video](https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset) data and [Youtube API](https://developers.google.com/youtube/v3)
    - `Youtube Trending Video` data is downloaded from [Kaggle.com](https://www.kaggle.com) with `.csv` file format, then loaded into `MySQL`, considered as a `data source`
    - Using `Video ID` and `Category ID` from `Youtube Trending Video` data, we collect some additional information fields from `Youtube API` such as `Video Link` and `Video Category`
 - **Extract Data**: Extract the above `data sources` using `Polars` `DataFrame`, now we have the `raw` layer, then load the data into `MinIO` `datalake`
 - **Tranform Data**: From `MinIO`, we use `Apache Spark`, specifically `PySpark`
    - convert from `Polars` `DataFrame` to `PySpark` `DataFrame` for processing and calculation, we get `silver` and `gold` layers
    - Data stored in `MinIO` is in `.parquet` format, providing better processing performance
 - **Load Data**: Load the `gold` layer into the `PostgreSQL` data warehouse, perform additional transform with `dbt` to create an `index`, making video searching faster
 - **Serving**: The data was used for visualization using `Metabase` and creating a video recommendation application using `Streamlit`
 - **package and orchestrator**: Use `Docker` to containerize and package projects and `Dagster` to coordinate `assets` across different tasks

## 📦 Technologies
 - `MySQL`
 - `Youtube API`
 - `Polars`
 - `MinIO`
 - `Apache Spark`
 - `PostgreSQL`
 - `Dbt`
 - `Metabase`
 - `Streamlit`
 - `Dagster`
 - `Docker`
 - `Apache Superset`
 - `Unittest`
 - `Pytest`

## 🦄 Features
Here's what you can do with:
 - You can completely change the logic or create new `assets` in the `data pipeline` as you wish, perform `aggregate` `calculations` on the `assets` in the `pipeline` according to your purposes.
 - You can also create new `data charts` as well as change existing `charts` as you like with extremely diverse `chart types` on `Metabase` and `Apache Superset`.
 - You can also create new or change my existing `dashboards` as you like
 - `Search` videos quickly with any `keyword`, for `Video Recommendation` Apps
 - `Search` in many different languages, not just `English` such as: `Japanese`, `Canadian`, `German`, `Indian`, `Russian`
 - Recommend videos based on `category` and `tags` video

## 👩🏽‍🍳 The Process


## 📚 What I Learned

During this project, I learned important skills, understood complex ideas, knew how to install and set up popular and useful tools, which brought me closer to becoming a `Data Engineer`.
 - **Logical thinking**: I learned how to think like a data person, find the cause of the data `problem` and then come up with the most `reasonable solution` to achieve high data `accuracy`.
 - **Architecture**: I understand and grasp the `ideas` and `architecture` of today's popular and popular big data processing tool, `Apache Spark`.
 - **Installation**: I learned how to install popular data processing, visualization and storage tools such as: `Metabase`, `Streamlit`, `MinIO`,... with `Docker`
 - **Setup**: I know how to setup `Spark Standalone Cluster` using `Docker` with three `Worker Nodes` on my local machine

### 📈 Overall Growth: 
Each part of this project has helped me understand more about how to build a data engineering, data management project. Learn new knowledge and improve my skills in future work

## 💭 How can it be improved?
 - Add more `data sources` to increase data richness.
 - Refer to other `data warehouses` besides `PostgreSQL` such as `Amazon Redshift` or `Snowflake`.
 - Perform more `cleaning` and `optimization` `processing` of the data.
 - Perform more advanced `statistics`, `analysis` and `calculations` with `Apache Spark`.
 - Check out other popular and popular `data orchestration` tools like `Apache Airflow`.
 - Separate `dbt` into a separate service (separate `container`) in `docker` when the project expands
 - Setup `Spark Cluster` on `cloud platforms` instead of on `local machines`
 - Refer to `cloud computing` services if the project is more extensive
 - Learn about `dbt packages` like `dbt-labs/dbt_utils` to help make the `transformation` process faster and more optimal.

## 🚦 Running the Project
To run the project in your local environment, follow these steps:
1. Run command after to clone the `repository` to your `local machine`.
~~~bash
   git clone https://github.com/longNguyen010203/Youtube-ETL-Pipeline.git
~~~

2. Run the following commands to build the images from the `Dockerfile`, pull images from `docker hub` and launch services
~~~bash
   make build
   make up
~~~

3. Run the following commands to access the `SQL editor` on the `terminal` and Check if `local_infile` was turned on
~~~python
   make to_mysql_root

   SET GLOBAL local_infile=TRUE;
   SHOW VARIABLES LIKE "local_infile";
   exit
~~~

4. Run the following commands to create tables with schema for `MySQL`, load data from `CSV` file to `MySQL` and create tables with schema for `PostgreSQL`
~~~bash
   make mysql_create
   make mysql_load
   make psql_create
~~~

5. Open [http://localhost:3001](http://localhost:3001) to view `Dagster UI` and click `Materialize all` button to run the Pipeline
6. Open [http://localhost:9001](http://localhost:9001) to view `MinIO UI` and check the data to be loaded
7. Open [http://localhost:8080](http://localhost:8080) to view `Spark UI` and three `workers` are running
8. Open [http://localhost:3030](http://localhost:3030) to see charts and `dashboards` on `Metabase`
9. Open [http://localhost:8501](http://localhost:8501) to try out the `video recommendation` app on `Streamlit`

## 🍿 Video
[Screencast from 2024-06-16 22-00-03.webm](https://github.com/longNguyen010203/Youtube-ETL-Pipeline/assets/168116061/33eb73b4-f4f0-48f6-9c2d-a4614e1f4ec5)
