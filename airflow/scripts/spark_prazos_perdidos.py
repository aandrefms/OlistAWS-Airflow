# Primeiramente, fazer unzip do arquivo do Olist ecommerce
from zipfile import ZipFile

# Criar um Objeto Zip e carregar brazilian-ecommerce.zip nele
with ZipFile('../../Data/Brazilian-ecommerce.zip',
             'r') as zipObj:
    zipObj.extractall()

# Configurando spark
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
conf = SparkConf().setMaster("local").setAppName("Prazos_Perdidos")
spark = SparkSession.builder.getOrCreate()
print(spark)


from pyspark.sql import SQLContext
sqlContext = SQLContext(spark)

# Editar Spark SQl context para facilitar uso com Pandas
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Criar uma spark session
spark = SparkSession.builder.getOrCreate()

# Carregar os arquivos csv em spark dataframes
df_items = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("olist_order_items_dataset.csv")

df_orders = spark.read.format("csv") \
             .option("header", "true") \
             .option("inferSchema", "true") \
             .load("olist_orders_dataset.csv")

df_products = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load("olist_products_dataset.csv")

# Criar SQL Table Views dos dataframes para SQL querying
df_items.createOrReplaceTempView('items')
df_orders.createOrReplaceTempView('orders')
df_products.createOrReplaceTempView('products')

# SQL Query que seleciona informações sobre order/seller/product de ordens onde
# o vendedor perdeu o prazo de entrega da remessa para a transportadora
entregas_atrasadas = spark.sql("""
SELECT i.order_id, i.seller_id, i.shipping_limit_date, i.price, i.freight_value,
       p.product_id, p.product_category_name,
       o.customer_id, o.order_status, o.order_purchase_timestamp, o.order_delivered_carrier_date,
       o.order_delivered_customer_date, o.order_estimated_delivery_date
FROM items AS i
JOIN orders AS o
ON i.order_id = o.order_id
JOIN products AS p
ON i.product_id = p.product_id
WHERE i.shipping_limit_date < o.order_delivered_carrier_date
""")

# Escrever os resultados em um único arquivo CSV
# coalesce(1) requires that the file is small enough to fit
# in the heap memory of the master Spark node and is therefore
# only recommended for very small datasets
# Alternatives are converting the Spark df to a Pandas df before
# writing to disk.
# Otherwise, it's best practice to maintain the partitions to
# take advantage of HDFS
entregas_atrasadas.coalesce(1) \
                       .write \
                       .option("header", "true") \
                       .csv("../../Data/missed_shipping_limit_orders.csv")
