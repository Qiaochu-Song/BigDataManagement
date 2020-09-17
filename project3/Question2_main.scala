import org.apache.spark.sql.SparkSession
// For implicit conversions like converting RDDs to DataFrames
//import spark.implicits._
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.functions
case class Customer (ID: Int, Name: String, Age: Int, Gender: String, CountryCode: Int, Salary: Float)
case class Transaction (transID: Int, CustID: Int, TransTotal: Float, TransNumItems: Int, TransDesc: String)

object SparkQ2 extends Serializable{
    def main(args: Array[String]): Unit = {  // <dir/to/customer.txt> <dir/to/"transaction.txt">
        val spark = SparkSession.builder().appName("question2").config("spark.some.config", "some-value").getOrCreate()

		// The customer file has 50k customers
		// ID: unique sequential number (integer) from 1 to 50,000 
		// Name: random sequence of characters of length between 10 and 20
		// Age: random number (integer) between 10 to 70
		// Gender: string that is either “male” or “female”
		// CountryCode: random number (integer) between 1 and 10
		// Salary: random number (float) between 100 and 10000


		// ********replace the url with the dir/to/transaction.txt********
		val custDF = sc.textFile(args(0)).map(_.split(",")).map(attributes => Customer(attributes(0).trim.toInt, attributes(1).trim, attributes(2).trim.toInt, attributes(3).trim, attributes(4).trim.toInt, attributes(5).trim.toFloat)).toDF()
		custDF.registerTempTable("customer")

		// The transaction file has 5M transactions
		// Schema:
		// TransID: unique sequential number (integer) from 1 to 5,000,000 
		// CustID: References one of the customer IDs, i.e., from 1 to 50,000
		// TransTotal: random number (float) between 10 and 1000
		// TransNumItems: random number (integer) between 1 and 10
		// TransDesc: random serial text of characters of length between 20 and 50
		val transDF = sc.textFile(args(1)).map(_.split(",")).map(attributes => Transaction(attributes(0).trim.toInt, attributes(1).trim.toInt, attributes(2).trim.toFloat, attributes(3).trim.toInt,attributes(4).trim)).toDF()  

		transDF.registerTempTable("transaction")

		// Report the number of male and the number of female customers in the country Z, 
		// where country Z is the country that have the largest sum of TransTotal for all its customers.
		val T1 = spark.sql("SELECT CountryCode, SUM(TransTotal) as count FROM transaction, customer WHERE transaction.CustID=customer.ID GROUP BY CountryCode ORDER BY count DESC LIMIT 1")
		T1.registerTempTable("T1")

		val T = spark.sql("SELECT COUNT(customer.ID) FROM customer, T1 WHERE customer.CountryCode = T1.CountryCode GROUP BY Gender")
		T.show()
    }
}

