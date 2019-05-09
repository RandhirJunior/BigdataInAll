# Databricks notebook source
my_df=spark.read.table('categories_csv')

# COMMAND ----------

my_df.show()

# COMMAND ----------

spark.sql('select * from categories_csv order by product_id').show()

# COMMAND ----------

order_items= spark.read\
.format("jdbc")\
.option("url","jdbc:mysql://ms.itversity.com")\
.option("dbtable","retail_db.orders")\
.option("user","retail_user")\
.option("password","itversity").load()

# COMMAND ----------

order_items.printSchema()

# COMMAND ----------



order_items.createOrReplaceTempView('order_items_tab')

# COMMAND ----------

spark.sql('show tables').show()

# COMMAND ----------

spark.sql('select order_item_order_id,order_item_quantity,rank() over(partition by order_item_order_id order by order_item_quantity desc) as total_rank from order_items_tab').show(50)

# COMMAND ----------

order_items.('order_items_tab').show()

# COMMAND ----------

sc

# COMMAND ----------

orderpath='categories.csv'
orderFile=open(orderpath)
type(orderFile)

# COMMAND ----------

print("hello world")

# COMMAND ----------

help(int)

# COMMAND ----------

s="Hello World"
print(type(s))
s[0]

# COMMAND ----------

order="1,2019-04-09 00:00:00.0,11234,CLOSED"
#order.split(",")[3].isalpha()
a=int(order.split(","[0]) if(order.split(",")[0].isdigit()) else 0

# COMMAND ----------

def sum1(lb,ub,f):
  total=0
  for i in range(lb,ub+1):
      total+=f(i)
  return total

sum1(5,10,lambda i:i*i)
  

# COMMAND ----------

sc



# COMMAND ----------

ordeItems=sc.textFile('dbfs:/FileStore/tables/orderitems.txt')

# COMMAND ----------


orderitemsfilter=ordeItems.filter(lambda oi:int(oi.split(",")[1])==2)

# COMMAND ----------

orderitemsfilter.collect()

# COMMAND ----------

orderitemap=orderitemsfilter.map(lambda oi:float(oi.split(",")[4]))

# COMMAND ----------

orderitemap.collect()

# COMMAND ----------

orderitemap.reduce(lambda x,y:x+y)

# COMMAND ----------

help(orderitemap)

# COMMAND ----------

sc

# COMMAND ----------

from pyspark import StorageLevel

# COMMAND ----------

ordeItems.persist(StorageLevel.MEMORY_ONLY)

# COMMAND ----------

ordeItems.count()

# COMMAND ----------

list2=ordeItems.take(10)

# COMMAND ----------

type(list2)

# COMMAND ----------

value=range(1,10)

# COMMAND ----------

print(value)

# COMMAND ----------

l=list(range(1,10001))

# COMMAND ----------

type(l)

# COMMAND ----------

help(sc.parallelize)

# COMMAND ----------

LRDD=sc.parallelize(l)

# COMMAND ----------

type(LRDD)

# COMMAND ----------

LRDD.take(20)

# COMMAND ----------

help(LRDD.filter)

# COMMAND ----------

EVEN=LRDD.filter(lambda x:x%2==0)

# COMMAND ----------

EVEN.reduce(lambda x,y: x+y)

# COMMAND ----------

import operator as op
EVEN.reduce(op.add)


# COMMAND ----------


ordeItems=sc.textFile('dbfs:/FileStore/tables/orderitems.txt')
orderItemMap=ordeItems.map(lambda oi:(int(oi.split(",")[1]),float(oi.split(",")[4])))
orderItemGBK=orderItemMap.groupByKey()
orderItemRBK=orderItemMap.reduceByKey(lambda x,y:x+y)

for i in orderItemGBK.take(10):print(i)
  


# COMMAND ----------

help(orderItemMap.groupByKey)

# COMMAND ----------

sc.setLogLevel("INFO")

# COMMAND ----------

hash("hi")

# COMMAND ----------

type(spark)

# COMMAND ----------

orderDF=spark.read.csv("dbfs:/FileStore/tables/orderitems.txt").toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")

# COMMAND ----------

from pyspark.sql.types import IntegerType
order_Cast=orderDF.withColumn("order_item_id",orderDF.order_item_id.cast(IntegerType())).withColumn("order_item_product_id",orderDF.order_item_product_id.cast(IntegerType()))

# COMMAND ----------

order_Cast.printSchema()

# COMMAND ----------

orderDF.describe().show()

# COMMAND ----------

orderDF.createTempView('orders')

# COMMAND ----------

spark.sql("select * from orders").show()

# COMMAND ----------

order_items=spark.read.jdbc("jdbc:mysql://ms.itversity.com","retail_db.orders",properties={"user":"retail_user","password":"itversity"})

# COMMAND ----------

order_items.show()

# COMMAND ----------

order_items1=spark.read.jdbc("jdbc:mysql://ms.itversity.com",
                            "retail_db.order_items"
                            ,numPartitions=4, 
                             column="order_item_order_id",
                             lowerBound='10000',
                             upperBound='20000',
                            properties={"user":"retail_user","password":"itversity"})
							
order_items1.rdd.getNumPartitions()
							
----------------------------------------------------------------------------------------------------------------------------------
from pyspark.sql.functions import *

order_items=spark.read.jdbc("jdbc:mysql://ms.itversity.com",
                            "retail_db.orders"
                            ,numPartitions=4, 
                            properties={"user":"retail_user","password":"itversity"})



order_items.select(substring('order_date',1,10).alias('oder_month')).show()

order_items.select(lower(order_items.order_status).alias("change_dta")).show()
order_items.withColumn('order_status',lower(order_items.order_status)).show()
order_items.withColumn('order_month',date_format('order_date','YYYMM')).show()
help(functions)
order_items.show()
order_items.selectExpr('case when order_status in("COMPLETE","CLOSED") then "COMPLETED" when order_status=="CANCLED" then "CANCLED" else "PENDING" end')

#-------------------------------DataFrame basic Operations--------------------

order_items.select('order_id','order_date','order_status').show()
order_items.select(order_items.order_id,order_items.order_date,order_items.order_status).show()
order_items.select(order_items.order_id,date_format(order_items.order_date,'YYY/MM'),order_items.order_status).show()

order_items.select('*').show()
-------------------------------------------------------withColumn(to gets all feilds along with tranformed filed)-------------------------------------------------------------------------------------------------

order_items.withColumn('order_status_old',lower(order_items.order_status)).show()
                        alisa name,actual column name
------------------------------------------------------------selectExpr-------------------------------------------------------------------

order_items.selectExpr("date_format(order_date,'YYYMM') as d_m").show()

-----------------------------------------------------------Filtering the data -------------------------------------------------------------------------------

order_items.filter(order_items.order_status=='COMPLETE').show()

or

order_items.filter("order_status=='COMPLETE'").show()


order_items.filter("order_status=='COMPLETE' or order_status='CLOSED'").show()

order_items.filter("order_status in('COMPLETE','CLOSED')").show()

order_items.where((order_items.order_status.isin('COMPLETE','CLOSED').__and__(date_format(order_items.order_date,'YYYMM')==201308) )).show()

order_items.where("order_status in ('COMPLETE','CLOSED') and order_date like '2013-08%'").show()

#order on every of 1st of month
order_items.where("date_format(order_date,'dd')='01'").show()

-------------------------------------------------ORDER_ITEMS TABLE-------------------------------------------------------------------


--------------------------------------------JOINING THE DATASETS-------------------------------------------------------------------
from pyspark.sql.functions import *

order_items=spark.read.jdbc("jdbc:mysql://ms.itversity.com",
                            "retail_db.order_items"
                            ,numPartitions=4, 
                            properties={"user":"retail_user","password":"itversity"})




order=spark.read.jdbc("jdbc:mysql://ms.itversity.com",
                            "retail_db.orders"
                            ,numPartitions=4, 
                            properties={"user":"retail_user","password":"itversity"})


order.where("order_status in ('COMPLETE','CLOSED')").join(order_items,order.order_id==order_items.order_item_id).show()


#--------------------Left Join-----

order.join(order_items,order.order_id==order_items.order_item_id,'left').show()

order_items.filter("order_item_order_id==2").agg(sum(order_items.order_item_subtotal)).show()

or 
order_items.filter(order_items.order_item_order_id==2).agg(sum(order_items.order_item_subtotal)).show()

order_items.groupBy("order_item_order_id").agg(round(sum("order_item_subtotal"),2)).show()

order.groupBy(order.order_status).count().show()

#----------------------------------Sorting the Data-------------------------------------------
order.sort(order.order_customer_id.asc()).show()

help(order.sort)

order.orderBy(order.order_date).show()

#composite sorting----------------
order.orderBy(order.order_date,order.order_status).show()

#-------------------------------Data Frame Operations - Analytics Functions or Windowing Functions----------------------

from pyspark.sql.window import Window

from pyspark.sql.functions import *
from pyspark.sql import functions


from pyspark.sql.types import *
OrderItemCsv=spark.read.csv("dbfs:/FileStore/tables/orderitems.txt").toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")

#OrderItemCsv.printSchema()
orderItems=OrderItemCsv.withColumn("order_item_id",OrderItemCsv.order_item_id.cast(IntegerType())).withColumn("order_item_order_id",OrderItemCsv.order_item_order_id.cast(IntegerType())).withColumn("order_item_product_id",OrderItemCsv.order_item_product_id.cast(IntegerType())).withColumn("order_item_quantity",OrderItemCsv.order_item_quantity.cast(IntegerType())).withColumn("order_item_subtotal",OrderItemCsv.order_item_subtotal.cast(FloatType())).withColumn("order_item_product_price",OrderItemCsv.order_item_product_price.cast(FloatType()))



						
						
