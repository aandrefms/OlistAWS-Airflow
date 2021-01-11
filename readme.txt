Este projeto utiliza dados reais de um e-commerce brasileiro e se baseia na necessidade da realização de análises e
modelagem constantes. Portanto, a execução deste projeto de Engenharia de Dados tem como objetivo criar uma pipeline
de ETL de forma programada/automática utilizando Python, SQL, Airflow, Spark e AWS.

Para isso, o script realizará os seguintes passos:

- Construir um data lake em AWS s3 com os dados do e-commerce brasileiro Olist
- Analisar as tabelas para verificar a perfomance de vendedores e solicitações
- Escrever um Spark job para identificar quais vendedores nao cumpriram os prazos de entrega dos produtos
  para a transportadora
- Construir uma ETL pipeline usando o Airflow que realize essas tarefas:
    - Fazer o download dos dados da AWS S3;
    - Executar o script/job Spark nestes dados afim de transformar-los em um dataset de solicitações com prazos
      perdidos;
    - Fazer o upload desse dataset novamente para a AWS S3 em um pasta com o potencial para análises avançadas.