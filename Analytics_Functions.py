#-------------------------------Data Frame Operations - Analytics Functions or Windowing Functions(Very useful for performace tunning it's reduce execution times)----------------------

from pyspark.sql.window import Window

from pyspark.sql.functions import *
from pyspark.sql import functions


from pyspark.sql.types import *
OrderItemCsv=spark.read.csv("dbfs:/FileStore/tables/orderitems.txt").toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")

#OrderItemCsv.printSchema()
orderItems=OrderItemCsv.withColumn("order_item_id",OrderItemCsv.order_item_id.cast(IntegerType())).withColumn("order_item_order_id",OrderItemCsv.order_item_order_id.cast(IntegerType())).withColumn("order_item_product_id",OrderItemCsv.order_item_product_id.cast(IntegerType())).withColumn("order_item_quantity",OrderItemCsv.order_item_quantity.cast(IntegerType())).withColumn("order_item_subtotal",OrderItemCsv.order_item_subtotal.cast(FloatType())).withColumn("order_item_product_price",OrderItemCsv.order_item_product_price.cast(FloatType()))

from pyspark.sql.functions import *
#orderItems.printSchema()
#orderItems.show()

orderItems.groupBy("order_item_order_id").agg(round(sum("order_item_subtotal"), 2)).show()


#################_______________________________________________________________________############################################################


spec=Window.partitionBy(orderItems.order_item_order_id)
type(spec)
 ##Out[31]: pyspark.sql.window.WindowSpec
 
 orderItems.select("order_item_order_id","order_item_subtotal",sum(orderItems.order_item_order_id).over(spec).alias("order_revenue")).show()
 orderItems.select("order_item_order_id","order_item_subtotal",round(orderItems.order_item_subtotal/sum(orderItems.order_item_order_id).over(spec),2).alias("order_revenue")).show()
orderItems.select("order_item_order_id","order_item_subtotal","order_item_product_id",round(orderItems.order_item_subtotal/sum(orderItems.order_item_order_id).over(spec),2).alias("order_revenue")).show()
	
	
#####################rank()#################################
spec=Window.partionedBy().orderBy()	
	
			
						
