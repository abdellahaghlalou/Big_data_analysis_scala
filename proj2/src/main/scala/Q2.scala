import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession, types}

object Q2 {
  val sc: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkII")
    .getOrCreate()
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

  def AreSales2013(Timestamp: String): Boolean = {
    Timestamp.contains("2013")
  }
  def main(args: Array[String]): Unit = {
    val Sales = getSales
    val Sales2013 = Sales.filter(Row => AreSales2013(Row(3).toString))
    Sales2013.show()
    val amount= Sales2013.select("amount").rdd.map(r=>r(0).asInstanceOf[Int]).collect()
    println(amount.sum)
  }
}
