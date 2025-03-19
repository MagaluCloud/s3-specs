import boto3 
import argparse
import os

def parser_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", 
                            required=True, 
                            default='*',
                            help="Path to the YAML config file")
    parser.add_argument("--endpoint", 
                        required=True, 
                        default='*',
                        help="Endpoint where the bucket will be created")
    parser.add_argument("--bucket",   
                    required=True,  
                    default='*',    
                    help="Bucket destination")   


    return parser.parse_args()


if __name__ == '__main__':
    parser = parser_arguments()
    path = './output/'

    # Initing boto3 client with info in ~/.aws/*
    session = boto3.Session(profile_name=parser.profile)
    client = session.client('s3', endpoint_url=parser.endpoint)

    # Parquet List will have regular updates with save name
    objs = os.listdir(path)
    parquet_list = [ob for ob in objs if '.parquet' in ob]

    try:
        for obj in parquet_list:
            print(f"Uploading {obj}")
            client.upload_file(Bucket = parser.bucket, Key=obj, Filename = path + obj)
    except Exception as e:
        raise e



    # Pdfs might be already on the bucket and their content are static

    response = client.list_objects_v2(Bucket=parser.bucket)
    pdf_set = set([ob['Key'] for ob in response['Contents'] if '.pdf' in ob['Key']])
    
    downloaded_pdfs_set = set([o for o in objs if '.pdf' in o])
    pdf_list = list(downloaded_pdfs_set.difference(pdf_set))

    try:
        for pdf in pdf_list:
            print(f"Uploading {obj}")
            client.upload_file(Bucket=parser.bucket, Key=pdf, Filename = path + 'reports/' + pdf)
    except Exception as e:
        raise e

