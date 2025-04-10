import boto3
import os
import sys
s3_client = boto3.client('s3')

async def upload_to_s3(bucket_name, data_path) :
    partition_key = f'cotacoes/'
    s3_key = f"Refined/{partition_key}{os.path.basename(data_path)}"
   
    try:
        s3_client.upload_file(data_path, bucket_name, s3_key)
        print(f"Arquivo {data_path} enviado para o bucket {bucket_name} na partição {s3_key}")
    except Exception as e:
        print(f"Erro ao enviar o arquivo {data_path} para o bucket {bucket_name} na partição {s3_key}")
        print(e)
        sys.exit(1)