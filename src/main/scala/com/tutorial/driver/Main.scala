package com.tutorial.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello")

    testSimpleQuery()
//    testAdventureWorksData()
  }

  def testAdventureWorksData(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("create_test_data")
      .master("local[4]")
      .getOrCreate()

    val query = "SELECT salesorderid, salesorderdetailid, carriertrackingnumber, orderqty, productid, specialofferid, unitprice, unitpricediscount, cast(rowguid as text), modifieddate FROM sales.salesorderdetail"

    val testDf = spark
      .read
      .option("DriverClass", "org.postgresql.Driver")
      .option("host", "localhost")
//      .option("host", "demo-database") // for docker compose
      .option("port", "5432")
      .option("user", "postgres")
      .option("password", "postgres")
      .option("schema", "Adventureworks")
      .option("partitions", "20")
      .format("com.tutorial.custom.datasource.DbDataReader")
      .load(query)

    println(s"Data count: ${testDf.count()}")
    testDf.show(truncate = false, numRows = 10)
    testDf.printSchema()

    Thread.sleep(1000000000)
  }

  def testSimpleQuery(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("create_test_data")
      .master("local[4]")
      .getOrCreate()

    val oneColSchema = StructType(Array(
      StructField("num", LongType, nullable = false)
    ))

    val query = "select num, txt from numbers_3"
//    val query = "select num from numbers"
//    val query = "select * from types_simple"

    val testDf = spark
      .read
      .option("DriverClass", "org.postgresql.Driver")
      .option("host", "localhost")
      .option("port", "5433")
      .option("user", "postgres")
      .option("password", "target")
      .option("partitions", "2")
//      .schema(oneColSchema)
      .format("com.tutorial.custom.datasource.DbDataReader")
      .load(query)
      .filter(col("num") < 10)
//      .filter((col("num") < 100) and (col("txt") === "a"))

    testDf.show(truncate = false)

    Thread.sleep(100000000)
  }
}
