# Databricks notebook source
df=spark.read.csv("/Volumes/workspace/default/adidas/Adidas US Sales Datasets.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

# DBTITLE 1,Aggregate sales metrics (fix cast error)
# MAGIC %sql
# MAGIC SELECT
# MAGIC   sum(cast(replace(replace(`Total Sales`, ',', ''), ' ', '') as double)) AS total_sales,
# MAGIC   sum(cast(replace(replace(`Operating Profit`, ',', ''), ' ', '') as double)) AS total_profit,
# MAGIC   avg(`Price per Unit`) AS avg_price_per_unit,
# MAGIC   sum(cast(replace(replace(`Units Sold`, ',', ''), ' ', '') as int)) AS total_units_sold
# MAGIC FROM sales_analysis;

# COMMAND ----------

# DBTITLE 1,Monthly total sales (fix cast error)
# MAGIC %sql
# MAGIC select
# MAGIC   sum(cast(replace(replace(`total sales`, ',', ''), ' ', '') as double)) as total_sales,
# MAGIC   date_format(`Invoice Date`, 'MMMM') as month
# MAGIC from sales_analysis
# MAGIC group by month

# COMMAND ----------

# DBTITLE 1,total sales in state
# MAGIC %sql select sum(cast(replace(replace(`total sales`, ',', ''), ' ', '') as double)),state  from  sales_analysis
# MAGIC group by 2
# MAGIC order by 1 desc limit 5 

# COMMAND ----------

# DBTITLE 1,total sales by region
# MAGIC
# MAGIC %sql select sum(cast(replace(replace(`total sales`, ',', ''), ' ', '') as double))total_sales,region  from  sales_analysis
# MAGIC group by 2

# COMMAND ----------

# DBTITLE 1,total sales buy product
# MAGIC %sql select sum(cast(replace(replace(`total sales`, ',', ''), ' ', '') as double))total_sales,Product
# MAGIC   from  sales_analysis
# MAGIC group by 2
# MAGIC order by 1 desc limit 5 

# COMMAND ----------

# DBTITLE 1,total sales by retailer
# MAGIC %sql select sum(cast(replace(replace(`total sales`, ',', ''), ' ', '') as double)) total_sales,retailer
# MAGIC   from  sales_analysis
# MAGIC group by 2
# MAGIC order by 1 desc limit 5 

# COMMAND ----------

# DBTITLE 1,unit sold by category (fix column name)
# MAGIC %sql
# MAGIC select sum(cast(replace(replace(`Units Sold`, ',', ''), ' ', '') as double)) total_unit_sold,`sales method`
# MAGIC from sales_analysis
# MAGIC group by 2

# COMMAND ----------

# DBTITLE 1,top performing cities by profit
# MAGIC %sql select sum(cast(replace(replace(`operating profit`, ',', ''), ' ', '') as double))  total_profit,city
# MAGIC from  sales_analysis
# MAGIC group by 2
# MAGIC order by 1 desc limit 5

# COMMAND ----------

