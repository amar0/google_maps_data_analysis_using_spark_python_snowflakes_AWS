import json
import requests
import pandas as pd
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    #google maps API key from all things dev website
    url = "https://Google-Maps-Data.proxy-production.allthingsdev.co/api/searchmaps.php?query=cat/dog+hostel&limit=50&country=in&lang=en&lat=12.972927&lng=77.601872&offset=0"

    headers = {
        'x-apihub-key': 'wU5NX17gF8SekT7hadF0SjIMPh-EMr1TgVfLBkxtkZdmukax9M',
        'x-apihub-host': 'Google-Maps-Data.allthingsdev.co',
        'x-apihub-endpoint': '44e57f32-acbb-4efd-b53c-91eed2bdd402'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")

    #creating boto3 object which is used to communicate with s3 and put objects into s3
    client = boto3.client('s3')
    filename = "google_data_raw_" + datetime.now().strftime("%Y%m%d%H%M%S") + '.json'
    client.put_object(
        Body=json.dumps(data), 
        Bucket='google-maps-etl-project-amar', 
        Key='raw_data/to_process/' + filename
    )
