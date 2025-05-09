# Databricks notebook source
# import libraries
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import month,year,quarter,sum,asc,desc,count,countDistinct


# COMMAND ----------

# create sales schema
schema=StructType([
    StructField("product_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True)
    ])


# COMMAND ----------

# Read sales csv
sales_df=spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/sales_csv.txt")

# COMMAND ----------

# add columns in sales
sales_df=sales_df.withColumn("order_year",year(sales_df.order_date))
sales_df=sales_df.withColumn("order_month",month(sales_df.order_date))
sales_df=sales_df.withColumn("order_quarter",quarter(sales_df.order_date))
display(sales_df)

# COMMAND ----------

# create menu schema
schema=StructType([
    StructField("product_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("price",StringType(),True),
    ])

# COMMAND ----------

# Read menu csv
menu_df=spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/menu_csv.txt")

# COMMAND ----------

display(menu_df)

# COMMAND ----------

# Total amount spent by each customer
# joining two datafrmaes
df_joined = sales_df.join(menu_df,sales_df["product_id"] == menu_df["product_id"], "inner")
df1=df_joined.groupBy("customer_id").agg(sum("price").alias("total_amount")).orderBy(asc("customer_id"))
display(df1)


# COMMAND ----------

# total amount spend by each food catagory
df2=df_joined.groupBy("product_name").agg(sum("price").alias("total_amount")).orderBy(asc("product_name"))
display(df2)


# COMMAND ----------

# total amount of sales in each month
df3=df_joined.groupBy("order_month").agg(sum("price").alias("total_amount")).orderBy(asc("order_month"))
display(df3)

# COMMAND ----------

#  yearly sales
df4=df_joined.groupBy("order_year").agg(sum("price").alias("total_amount")).orderBy(asc("order_year"))
display(total_amount)


# COMMAND ----------

# how many times each product purchased
df5 = df_joined.groupBy("product_name").agg(count("*").alias("purchase_count")).orderBy("purchase_count")
display(df5)


# COMMAND ----------

# top 5 ordered items
df6= df_joined.groupBy("product_name").agg(count("*").alias("purchase_count")).orderBy(desc("purchase_count")).limit(5)
display(df6)



# COMMAND ----------

# top ordered items'
df7= df_joined.groupBy("product_name").agg(count("*").alias("purchase_count")).orderBy(desc("purchase_count")).limit(1)
display(df7)



# COMMAND ----------

# frequency of customer visited to restaurant
df8=(sales_df.filter(sales_df.source_order=="Restaurant").groupBy("customer_id").agg(countDistinct("order_date")))
display(df8)


# COMMAND ----------

# total sales by each country
df9=df_joined.groupBy("location").agg(sum("price")).alias("total_sales")
display(df9)


# COMMAND ----------

