import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession, types}



object Q1 {
  //val sc: SparkSession = sc
  val sc: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkII")
    .getOrCreate()
  def getProduct: org.apache.spark.sql.DataFrame ={
    val product = "C:\\Users\\abdel\\Downloads\\Projet Scala-20210121\\Product.txt"
    val Product_scheme = StructType(Array(
      StructField("prodID", IntegerType, false),
      StructField("name", StringType, false),
      StructField("type", StringType, false),
      StructField("version", StringType, true),
      StructField("price", IntegerType, false)
    ))
    val Product = sc.read
      .option("sep", "|")
      .option("header", true)
      .schema(Product_scheme)
      .csv(product)
    return Product
  }
  def getSales: org.apache.spark.sql.DataFrame ={
    val sales = "C:\\Users\\abdel\\Downloads\\Projet Scala-20210121\\Sales.txt"
    val Sales_scheme = StructType(Array(
      //txID|custID|prodID|timestamp|amount|quantity
      StructField("txID", IntegerType, false),
      StructField("custID", IntegerType, false),
      StructField("prodID", IntegerType, false),
      StructField("timestamp", StringType, false),
      StructField("amount", IntegerType, false),
      StructField("quantity", IntegerType, false)
    ))
    val Sales = sc.read
      .option("sep", "|")
      .option("header", true)
      .schema(Sales_scheme)
      .csv(sales)
    return Sales
  }


  def main(args: Array[String]): Unit = {
    val Sales:org.apache.spark.sql.DataFrame = getSales
    val Products:org.apache.spark.sql.DataFrame=getProduct
    Sales.columns.foreach(println(_))
    var SalesDistr =Sales.groupBy("prodID").sum("amount","quantity")
    SalesDistr = SalesDistr.alias("n").join(Products,SalesDistr("prodID")===Products("prodID"),"inner")
      .select(col("n.prodID"),
        col("name"),
        col("sum(amount)").as("Total amount"),
        col("sum(quantity)").as("Total quantity"))
    SalesDistr.show()
  }
}
