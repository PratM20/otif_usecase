// Databricks notebook source

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.parallel.ParMap
import org.apache.spark.sql.types._


class GenericReader {

  class DataReader {
    private case class LazyCsv(path: String, flavour: String)

    private[this] val options = new mutable.HashMap[String, Any]
    // CSV options
      val infer_schema = "true"
      val first_row_is_header = "true"
      val delimiter = ","

    /**
     *
     * @param key : Key used to fetch dataframe from the map. Type - String
     * @param path : It is a file path.
     * @param flavour : It determines the type of file. Currently supported values are csv.
     * @return : object of DataReader
     */
    
    
    def addfile(key: String, path: String, flavour: String = "csv"): DataReader = synchronized {
      lazy val lazymap = LazyCsv(path, flavour)
      options += key -> lazymap
      this
    }
    
    /**
     * run function starts the execution of scanning files added via addfile function.
     * @param spark : Object of SparkSession
     * @return : It return a map of keys and dataframe passed through addfile function.
     */


    def run(spark: SparkSession): ParMap[String, DataFrame] = synchronized {

      
      options.par.map(f => {

        val df_ret = {
          val lzycsv = f._2.asInstanceOf[LazyCsv]
          val df = spark.read.format(lzycsv.flavour)
                      .option("inferSchema", infer_schema)
                      .option("header", first_row_is_header)
                      .option("sep", delimiter)
                      .load(lzycsv.path)
          val df_with_newColumns = spark.createDataFrame(df.rdd, StructType(df.schema.map(s => StructField(s.name.replaceAll(" ", "_"), s.dataType, s.nullable))))
          df_with_newColumns
        }
        (f._1, df_ret)

      }).toMap
    }

  }

  class DataWriter {
    
    private case class LazyDfCsv(df: DataFrame, flavour: String, part: Integer, table: String)
    private[this] val options = new mutable.HashMap[String, Any]
    
     /**
     *
     * @param key : Key used to fetch dataframe from the map. Type - String
     * @param df : Dataframe to write. Type - DataFrame
     * @param flavour : It determines the type of file. Currently supported values are csv.Type - String
     * @part : repartition value.Type - Integer
     * @table : table name.Type - String
     * @return : object of DataWriter
     */
    
    
     def addDataframe(key: String, df: DataFrame, flavour: String = "csv" , part: Integer, table: String): DataWriter = synchronized {
      lazy val lazymap = LazyDfCsv(df, flavour,part,table)
      options += key -> lazymap
      this
    }
    
      /**
     * run function starts the execution of scanning files added via addDataframe function.
     * @param spark : Object of SparkSession
     */
    
    def run(spark: SparkSession) = synchronized {

      
      options.par.map(f => {

          val lzycsv = f._2.asInstanceOf[LazyDfCsv]
          val df = lzycsv.df
          df.repartition(lzycsv.part).write.format(lzycsv.flavour).saveAsTable(lzycsv.table)

    })
  }                     

}
    def readBuilder(): DataReader = new DataReader
    def writeBuilder(): DataWriter = new DataWriter
}

object GenericReader extends GenericReader {}
