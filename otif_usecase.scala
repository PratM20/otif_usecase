// Databricks notebook source
// DBTITLE 1,Task 1a (Loading data in parallel and writing in parallel)


// COMMAND ----------

// DBTITLE 0,Task 1a (Loading data in parallel and writing in parallel)
/* Calling to generic reader class. The function inside the classs is used to make read and write parallel */
%run /Users/prateek20sep@gmail.com/generic_reader

// COMMAND ----------

/* Calling reader function .  Reading the objects in parallel to save time */
val delivery_file_location = "/FileStore/shared_uploads/prateek20sep@gmail.com/OTIF_DATA_deivery_note.csv"
val sales_order_file_location = "/FileStore/shared_uploads/prateek20sep@gmail.com/OTIF_DATA_Sales_order.csv"




val readerMap = GenericReader.readBuilder()
               .addfile("OTIF_DATA_deivery_note", delivery_file_location, "csv")
               .addfile("OTIF_DATA_Sales_order", sales_order_file_location, "csv")
               .run(spark)  

val deliveryDf = readerMap.get("OTIF_DATA_deivery_note").get
val salesOrderDf = readerMap.get("OTIF_DATA_Sales_order").get

// COMMAND ----------

    /**
     * numPart function calculates partition count
     * @param df : Dataframe
     * @return : It returns Integer.
     */


def numPart (df : DataFrame) : Integer ={
import scala.math.BigInt
salesOrderDf.cache.foreach(_ => ())
val catalyst_plan = salesOrderDf.queryExecution.logical
val df_size_in_bytes = spark.sessionState.executePlan(catalyst_plan).optimizedPlan.stats.sizeInBytes
val numPart = (df_size_in_bytes/(64 * 1024*1024)).toInt
if (numPart == 0) 1 else numPart    
  
}
//salesOrderDf.repartition(1).write.format("csv").saveAsTable("OTIF_DATA_Sales_order")

// COMMAND ----------

/* Calling writer function .  Writing the objects in parallel to save time */

val writerMap = GenericReader.writeBuilder()
               .addDataframe("OTIF_DATA_deivery_note", deliveryDf, "csv" ,numPart(deliveryDf),"OTIF_DATA_deivery_note")
               .addDataframe("OTIF_DATA_Sales_order", salesOrderDf, "csv",numPart(salesOrderDf),"OTIF_DATA_Sales_order")
               .run(spark)  

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from OTIF_DATA_deivery_note where Sales_Order_Document=33355552983001

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from otif_data_sales_order where Sales_Order_Document=33355552983001

// COMMAND ----------

// DBTITLE 1,Task 1b (Calculating OTIF)


// COMMAND ----------

/* Preparing base data . Summing up delivered qty corresponding to Sales_Order_Document */
val intermediate_1_df = spark.sql("""
select 
a.Sales_Order_Document,
a.Sales_Organization,
a.Sales_Order_Qty,
a.Article,
a.Size_Grid_Value,
to_date(a.Requested_Delivery_Date, "dd/MM/yyyy") as Requested_Delivery_Date,
to_date(b.Actual_Delivery_Date, "dd/MM/yyyy") as Actual_Delivery_Date,
SUM(b.Delivery_Qty) OVER (PARTITION BY a.Sales_Order_Document) AS Delivery_Qty_Total,
b.Delivery_Qty

from
otif_data_sales_order  a
INNER JOIN
OTIF_DATA_deivery_note b
ON( a.Sales_Order_Document =  b.Sales_Order_Document
AND a.Sales_Organization = b.Sales_Organization) """)

intermediate_1_df.createOrReplaceTempView("intermediate_1_df")

// COMMAND ----------

/* Calculating order delivered within 2 days delay*/

val intermediate_2_df = spark.sql("""
select 
Sales_Order_Document,
Article,
Size_Grid_Value,
CASE WHEN Actual_Delivery_Date < date_add(Requested_Delivery_Date,2) THEN 1 ELSE 0 END AS otif_ind,
ROUND((Delivery_Qty_Total/Sales_Order_Qty),2) as otif_percent,
Requested_Delivery_Date,
Actual_Delivery_Date,
Delivery_Qty,
Delivery_Qty_Total,
Sales_Order_Qty
FROM
intermediate_1_df  """)

intermediate_2_df.createOrReplaceTempView("intermediate_2_df")

// COMMAND ----------

//Final data
val otif_df = spark.sql("""
select Sales_Order_Document,
Article,
Size_Grid_Value,
Delivery_Qty_Total,
Sales_Order_Qty,
Requested_Delivery_Date,
Actual_Delivery_Date,
otif_percent

FROM
intermediate_2_df WHERE otif_ind = 1 AND otif_percent > 0.9 """)


// COMMAND ----------

val writerMap = GenericReader.writeBuilder()
               .addDataframe("OTIF_DATA", otif_df, "csv" ,1,"OTIF_DATA")
               .run(spark)  

// COMMAND ----------

// Monthly data dump
%sql

select date_format(Actual_Delivery_Date,'yyyy-MM') as month,  sum(Delivery_Qty_Total)/sum(Sales_Order_Qty) as otif_monthly_percent from OTIF_DATA group by date_format(Actual_Delivery_Date,'yyyy-MM') order by month
