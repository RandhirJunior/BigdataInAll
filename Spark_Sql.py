########################################Spark SQL#################################################

from pyspark.sql.types import *
OrderItemCsv=spark.read.csv("dbfs:/FileStore/tables/orderitems.txt").toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")

#OrderItemCsv.printSchema()
orderItems=OrderItemCsv.withColumn("order_item_id",OrderItemCsv.order_item_id.cast(IntegerType())).withColumn("order_item_order_id",OrderItemCsv.order_item_order_id.cast(IntegerType())).withColumn("order_item_product_id",OrderItemCsv.order_item_product_id.cast(IntegerType())).withColumn("order_item_quantity",OrderItemCsv.order_item_quantity.cast(IntegerType())).withColumn("order_item_subtotal",OrderItemCsv.order_item_subtotal.cast(FloatType())).withColumn("order_item_product_price",OrderItemCsv.order_item_product_price.cast(FloatType()))

#create view on dataframe
orderItems.createTempView("OrderItem_view")

#to get the data 
spark.sql("select * from OrderItem_view").show()

spark.sql('show tables').show()

spark.sql('create table my_new as select * from OrderItem_view ').show()

# to see the decription of tables  
spark.sql('describe formatted my_new ').collect()
#NOte: The sql query will internally convertd into DataFrame
df=spark.sql('select * from OrderItem_view')
type(df)

#o/p pyspark.sql.dataframe.DataFrame

#All functions related to sql

for i in spark.sql('show  functions').collect(): print(i)

help(spark.read)

help(df.write.saveAsTable)

help(df.write.insertInto)  # To write data into another hive table

help(spark)  #All API which are avaialable on top of Spark

help(spark.read)  #All API related to read data from various files 

help(df.write)  # To know all API about write

#to know current date
spark.sql('select date_format(current_date,"YYYYMM")').show()

#In aggregation we need to mentioned partionedBy clause 

spark.sql('select o.order_item_id,o.order_item_order_id,sum(order_item_subtotal) over (partition by)')

#--------------NOTE- rank() doesnt take any arguments and we need to sort the data because rank() function will not work properly----------------------------
spark.sql('select order_item_id,order_item_order_id
            ,order_item_subtotal,rank() over (partition by order_item_order_id order by order_item_subtotal desc) rnk from orderItems')
			
			
			
#NOTE:- Analytics functions are used in select clause only			
