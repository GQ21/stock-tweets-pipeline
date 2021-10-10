# Stock Tweets Batch Pipeline

<img src="https://github.com/GQ21/stock-tweets-pipeline/blob/main/images/img_stockstweets.png" align="centre">

## About The Project

This project aim is to aggregate twitter data relevant to NASDAQ stock market prices and give analytic visuals of people reactions. For simplicity I aggregated only three tickers: APPL, GOOG, AMZN. This pipeline is an imitation of big data batch pipeline and was created for learning purpose to understand how does AWS cloud services functions and explore best practices of how to collect, transform, load and visualize big data.

Link to web app: http://stwitterdashapp-env.eba-yxjupvvn.eu-north-1.elasticbeanstalk.com/

## App Interactivity

To interact with dash app use dropdown meniu to select ticker and daterange to show desired date period. For closer time period investigation zoom into bar plots graph.

<img src="https://github.com/GQ21/stock-tweets-pipeline/blob/main/images/gif_interaction.gif" align="centre">

## Architecture

<img src="https://github.com/GQ21/stock-tweets-pipeline/blob/main/images/img_pipeline.png" align="centre">

Data is produced from Twitter streaming api and yahoo financial socket with yliveticker then sent to Kafka broker were it is consumed and sent to S3 landing bucket.Airflow every night at 3 am creates EMR cluster with required [libraries](https://github.com/GQ21/stock-tweets-pipeline/blob/main/emr/EMR-install-libraries.sh), does ETL spark jobs and ingestion to readshift warehouse.After jobs completion cluster is being terminated, which allows to save extra money: 

<img src="https://github.com/GQ21/stock-tweets-pipeline/blob/main/images/img_airflow.png" align="centre">

Dash app is set up to update after 5 am on the first app load. 

## ETL

EMR cluster performs ETL steps as follows:

* Collect tweets and tickers JSON data taken from s3 landing bucket and export it as CSV to working bucket.
* Take data from working bucket process it and export to process bucket.
* Take data from process bucket and make tweets sentiment predictions, export sentiments as new CSV to process bucket.
* Perform data quality checks on all dataframes.
* Ingest data into redshift staging schema.
* Upsert data into redshift analytic schema.

## Data quality Checks

Here are a few simple quality checks that EMR performs before warehouse ingestion:
* Checks if dataframe columns line up in correct order.
* Checks if columns has correct data types.
* Checks if tweets and sentiments dataframes doesn't have any duplicates.
* Checks if tweets and stocks dataframes are not exceeding yesterdays time range.

## NLP Model 

For tweets sentiment predictions it was used very basic logistic regression [model](https://github.com/GQ21/stock-tweets-pipeline/blob/main/emr/jobs/inference/model/simple_model.ipynb) with TF–IDF vectorizer. Model was trained on kaggle's found
[data](https://www.kaggle.com/utkarshxy/stock-markettweets-lexicon-data).

## Data Model

### Staging

For redshift staging data model was used Snowflake Schema. It is basically Star Schema without tickers_info table where main tweets table contains all facts associated to each tweet and four dimensions tables. It is convient set up, because this model enables to search the database schema with the minimum number of SQL JOINs possible and enable fast read queries.

<img src="https://github.com/GQ21/stock-tweets-pipeline/blob/main/images/img_staging.png" align="centre">

### Analytic

Analytic schema consists of six seperate tables which can be devided to three sections containing three different tickers information.

<img src="https://github.com/GQ21/stock-tweets-pipeline/blob/main/images/img_analytic.png" align="centre">

## Scalability 

EMR: 
*   If needed EMR cluster can be set to auto scaling in airflow dag EMR cluster configuration by specifying ManagedScalingPolicy and activating AutoScalingRole.

Redshift: 
*    Data can be access by 500+ connections and 50 concurrency per cluster. To improve performance redshift can be easily scaled to get faster query results for each user.

Eastic BeanStalk:
*   Beanstalk environment includes an Auto Scaling group that manages the Amazon EC2 instances. Auto scaling group can be easily modified by changing min,max and type of instances needed.

## Room for improvement

* NLP model can be improved a lot by testing various other models and text processing algorithms. Most likely finding additional labelled data would be a great boost in model metrics.
* For the sake of simplicity and financial cost I used kafka and airflow localy although they as well can be put in aws cloud environment.
* Eventhough this is a batch pipeline by using same technologies it can be extended to lambda architecture so that both batch and live data processing methods could be used.

## Tools and Technologies used

* Python 3.6
* Yliveticker
* Tweepy
* Kafka
* AWS Redhift
* AWS EMR
* AWS S3
* AWS IAM
* AWS Elastic Beanstalk
* AWS Security Manager
* Apache Airflow
* Dash
* Scikit-learn
* Spark 
* Docker

## Status

Finished.  \
Pipeline started 2021-09-27 and was stopped 2021-10-02.

## License

This project is licensed under MIT [license](https://github.com/GQ21/stock-tweets-pipeline/blob/main/LICENSE)