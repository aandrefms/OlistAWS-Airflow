from boto3.s3.transfer import S3Transfer
import boto3

# Editar as chaves de acesso
k_acess = 'sua_acess_key'
k_secret = 'sua_secret_key'
nome_s3_bucket = 'seu_s3_bucket_name'
s3_arquivo = 'brazilian-ecommerce.zip'
download_path = '../../Data/'

client = boto3.client('s3',
                      aws_access_key_id=k_acess,
                      aws_secret_access_key=k_secret)

client.download_file(nome_s3_bucket, s3_arquivo, download_path)