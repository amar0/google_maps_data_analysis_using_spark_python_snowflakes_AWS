Google Maps data anlysis using Spark, Python, Snowflakes and AWS
------------------------------------------------------------------

Overview
------------------------------------------------------------------
This project aims to perform analysis on the Google Maps data based on the specific lat/long details and categories.

Project Goals
----------------
1. Data Extraction — Build a mechanism to extract data from maps API
2. ETL System — We are getting data in raw format, transforming this data into the proper format
3. Snowflake — We will be getting data from API and store it on to Snowflake Data Warehouse.
4. Cloud — We can’t process vast amounts of data on our local computer so we need to use the cloud, in this case, we will use AWS
5. Frontend — Build a Flask application to fetch data from Snowflake and display in an user readible format.

Services used
---------------
1. Amazon S3: Amazon S3 is an object storage service that provides manufacturing scalability, data availability, security, and performance.
2. AWS IAM: This is nothing but identity and access management which enables us to manage access to AWS services and resources securely.
3. AWS Glue: A serverless data integration service that makes it easy to discover, prepare, and combine data for analytics, machine learning, and application development.
4. AWS Lambda: Lambda is a computing service that allows programmers to run code without creating or managing servers.
5. Snowflake Data Warehouse: A Data Warehouse is a relational database designed for analytical work. The Snowflake Data Cloud includes a pure cloud, SQL data warehouse

Architecture Diagram
-----------------------
<img width="745" alt="Screenshot 2025-01-01 at 21 52 47" src="https://github.com/user-attachments/assets/b27815a8-210c-43c9-aed1-e543da976b94" />

Explaination
-----------------------
Firstly we get the API URL and the header details from All Things Dev website (https://www.allthingsdev.co/apimarketplace/endpoints/google-maps-data/675c452e33e2ef7a2c207fbd) and once we have that we write Extract script in Python 3.0 as writing extraction in python is much more cost efficient than writing in spark.

Once the extraction script is ready we deploy the code on AWS lambda (serverless service) and use boto3 object to put the extracted data onto s3 bucket (s3 bucket stores data as objects) as raw data.

Next step would be to write spark transformation on AWS Glue notebook and put transformed data onto another s3 bucket. We need to add cloud watch trigger to automatically run the job based on business requirements.

Final step would be to create the integration between snowflake and AWS s3 along with creating the file format and stage object to load data from each json files from s3 and load onto snowflake table.
