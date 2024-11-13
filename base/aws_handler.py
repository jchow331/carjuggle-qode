from decouple import config
import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd
from io import StringIO
import os

class AWSHandler:
    """
     from AWS.Aws.AwsHandler import AwsHandler
     a=AwsHandler()
     a.upload_to_aws("try.txt","MasterCodeUS/scraping/autolist")
     a.download_from_aws("MasterCodeUS/scraping/autolist/try.txt","try_download.txt")

     pip install python-decouple
     change filename example.env to .env
    """
    def __init__(self):
        self.AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")
        self.BUCKET_NAME = config("AWS_STORAGE_BUCKET_NAME")
        self.AWS_REGION = config("AWS_S3_REGION_NAME")
        self.s3 = boto3.client('s3', aws_access_key_id=config("AWS_ACCESS_KEY_ID"),
                               aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY"))

    def upload_to_aws(self, file_path, destination_folder_name):
        """
        @param file_path: local path of the Csv
        @param bucket: Bucket Name
        @param destination_folder_name: Folder where csv should be updated
        @return: Sucess boolean
        """
        try:
            filename = os.path.split(file_path)[-1]
            self.s3.upload_file(file_path, self.BUCKET_NAME, f'{destination_folder_name}/{filename}')
        except FileNotFoundError:
            raise
        except NoCredentialsError:
            raise

    def download_from_aws(self, target_file_path, save_path):
        """
        target_file_path -- The name of the key to download from.
        save_path-- The path to the file to download to in local machine.
        @return:
        """
        try:
            self.s3.download_file(self.BUCKET_NAME, target_file_path, save_path)
        except FileNotFoundError:
            raise
        except NoCredentialsError:
            raise

    def download_object(self, target_file_path):
        """
        @param target_file_path: The name of the key to download from.
        @return: data
        doc_source:https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#:~:text=download_fileobj(Bucket%2C%20Key,is%20a%20managed
        """
        obj = self.s3.get_object(Bucket=self.BUCKET_NAME, Key=target_file_path)
        return obj

    def download_object_as_csv(self, target_file_path):
        """
        @param target_file_path: the name of the csv to download from
        @return: a pandas dataframe
        """
        obj = self.s3.get_object(Bucket=self.BUCKET_NAME, Key=target_file_path)
        df = pd.read_csv(obj['Body'])
        return df

    def upload_csv_object(self, dataframe, destination_file_path):
        """
        @param data: pandas object
        @param destination_file_path: whole path to upload to including the file_name
        @return:
        """
        csv_buffer = StringIO()
        csv_buffer.seek(0)
        dataframe.to_csv(csv_buffer)
        self.s3.put_object(Body=csv_buffer.getvalue(), Bucket=self.BUCKET_NAME, Key=destination_file_path)


    def get_folder_bucket_files(self, destination_folder_path):
        """
        @param destination_file_path: whole path of the folder within the bucket
        @return: list of csv file paths
        """
        my_bucket_objects=self.s3.list_objects(Bucket=self.BUCKET_NAME, Prefix=destination_folder_path)
        my_bucket_contents=my_bucket_objects['Contents']
        
        files_paths=[]
        for my_bucket_content in my_bucket_contents:
            if my_bucket_content['Key'].endswith('.csv'):
                files_paths.append(my_bucket_content['Key'])

        return files_paths
