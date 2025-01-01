###Google Maps data anlysis using spark, python, snowflakes and AWS

  <img width="745" alt="Screenshot 2025-01-01 at 21 52 47" src="https://github.com/user-attachments/assets/b27815a8-210c-43c9-aed1-e543da976b94" />


Firstly we get the API URL and the header details from All Things Dev website (https://www.allthingsdev.co/apimarketplace/endpoints/google-maps-data/675c452e33e2ef7a2c207fbd) and once we have that we write Extract script in Python 3.0 as writing extraction in python is much more cost efficient than writing in spark.

Once the extraction script is ready we deploy the code on AWS lambda (serverless service) and use boto3 object to put the extracted data onto s3 bucket (s3 bucket stores data as objects) as raw data.

Next step would be to write spark transformation on AWS Glue notebook and put transformed data onto another s3 bucket. We need to add cloud watch trigger to automatically run the job based on business requirements.

Final step would be to create the integration between snowflake and AWS s3 along with creating the file format and stage object to load data from each json files from s3 and load onto snowflake table.
