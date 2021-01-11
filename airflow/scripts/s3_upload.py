import os
from boto3.s3.transfer import S3Transfer
import boto3

# Editar as chaves de acesso
k_acess = 'sua_acess_key'
k_secret = 'sua_secret_key'
nome_s3_bucket = 'seu_s3_bucket_name'
s3_arquivo = 'brazilian-ecommerce.zip'
s3_arquivo_csv = 'missed_shipping_limit_orders.csv'
client = boto3.client('s3',
                      aws_access_key_id=k_acess,
                      aws_secret_access_key=k_secret)

transfer = S3Transfer(client)

print('transfer - ' + nome_s3_bucket)


# Definir função para analisar pelo diretório de saida do Spark.
# Identifica arquivos csv e faz o upload no S3 bucket
def enviarDiretorio(filepath, s3_bucket_name):
    for root, dirs, files in os.walk(filepath):
        for file in files:
            # Transfere apenas arquivos csv
            if file.endswith('csv'):
                transfer.upload_file(os.path.join(root, file),
                                     s3_bucket_name,
                                     "Clean_Data/" + file)  # Arquivo inserido na pasta clean-data


enviarDiretorio(filepath=s3_arquivo_csv, s3_bucket_name=nome_s3_bucket)
