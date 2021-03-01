import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession, types}
import org.apache.spark.sql.functions.col



object Q3 {

  val sc: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkII")
    .getOrCreate()

  def getCustomer: org.apache.spark.sql.DataFrame ={
    val customer = "C:\\Users\\abdel\\Downloads\\Projet Scala-20210121\\Customer.txt"
    val Customer_scheme = StructType(Array(
      StructField("CustID", IntegerType, false),
      StructField("Firstname", StringType, false),
      StructField("Lastname", StringType, false),
      StructField("Phone", StringType, true)
    ))
    val Customer = sc.read
      .option("sep", "|")
      .option("header", true)
      .schema(Customer_scheme)
      .csv(customer)
    return Customer
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
    val Customer = getCustomer
    val Sales = getSales
    val SalesperCustomerID = Sales.groupBy("custID").sum("quantity")
    val SalesperCustomer = SalesperCustomerID.alias("SId")
      .join(Customer.alias("c"),Customer("custID")===SalesperCustomerID("custID"),"inner")
      .select(col("c.custID"),
        col("c.Firstname"),
        col("c.Lastname"),
        col("SId.sum(quantity)").as("Total number of products"))
    SalesperCustomer.show()
  }
}