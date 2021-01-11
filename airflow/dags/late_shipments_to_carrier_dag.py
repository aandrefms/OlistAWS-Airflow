from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG('carregamentos_atrasados',
          description='Retorna uma lista dos pedidos onde o vendedor perdeu o prazo da transportadora',
          schedule_interval='0 5 * * *',
          start_date=datetime(2019, 7, 10), catchup=False)

# Download dos dados da Amazon S3
s3_download_operator = BashOperator(task_id='s3_download',
                                    bash_command='python ../scripts/s3_download.py',
                                    dag=dag)

# Executar os comandos Pyspark para retornar as solicitações
# onde o vendedor perdeu o prazo de entrega a transportadora
spark_prazo_operator = BashOperator(task_id='spark_missed_deadline_job',
                                              bash_command='python ../scripts/spark_missed_deadline_job.py',
                                              dag=dag)

# Especificar que a task acima depende que o download dos dados ocorra
spark_prazo_operator.set_upstream(s3_download_operator)

# Fazer o upload dos dados tratados para a amazon S3
s3_upload_operator = BashOperator(task_id='s3_upload',
                                  bash_command='python ../scripts/s3_upload.py',
                                  dag=dag)

# Especificar que o upload depende do Pyspark job funcionar normalmente
s3_upload_operator.set_upstream(spark_prazo_operator)

s3_download_operator >> spark_prazo_operator >> s3_upload_operator
