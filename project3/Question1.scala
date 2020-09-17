import org.apache.spark.sql.SparkSession
// For implicit conversions like converting RDDs to DataFrames
//import org.apache.spark.implicits._
//import sqlContext.implicits._
case class Transaction (transID: Int, CustID: Int, TransTotal: Float, TransNumItems: Int, TransDesc: String)
object SparkQ1 extends Serializable{
    
    def main(args: Array[String]): Unit = {

	    val spark = SparkSession.builder().appName("question1").config("spark.some.config", "some-value").getOrCreate()
	// The file has 5M transactions
	// Schema:
	// TransID: unique sequential number (integer) from 1 to 5,000,000 
	// CustID: References one of the customer IDs, i.e., from 1 to 50,000
	// TransTotal: random number (float) between 10 and 1000
	// TransNumItems: random number (integer) between 1 and 10
	// TransDesc: random serial text of characters of length between 20 and 50

	//case class Transaction (transID: Int, CustID: Int, TransTotal: Float, TransNumItems: Int, TransDesc: String)
	// in the spark shell the sc is already defined as sparkContext

	// ********replace the url with the dir/to/transaction.txt********
	    //import org.apache.spark.implicits._
	    val transDF = sc.textFile(args(0)).map(p=>p.split(",")).map(attributes => Transaction(attributes(0).trim.toInt, attributes(1).trim.toInt, attributes(2).trim.toFloat, attributes(3).trim.toInt,attributes(4).trim)).toDF()

	    transDF.registerTempTable("transaction")

	// T1: Filter out (drop) the transactions from T whose total amount is less than $200
		val T1 = spark.sql("SELECT * FROM transaction WHERE TransTotal > 200")
		T1.registerTempTable("T1")
		// Over T1, group the transactions by the Number of Items it has, and 
		// for each group calculate the sum, avg, the min and the max of the total amounts.
		val T2 = spark.sql("SELECT SUM(TransTotal), AVG(TransTotal), MIN(TransTotal), MAX(TransTotal) FROM T1 GROUP BY TransNumItems")

		// report T2 back to client side
		T2.show()

		// Over T1, group the transactions by customerID, and for each group report the customer ID, and the transactions’ count.
		val T3 = spark.sql("SELECT CustID, COUNT(transID) as transCount FROM T1 GROUP BY CustID")
        T3.registerTempTable("T3")
		// Filter out (drop) the transactions from T whose total amount is less than $600
		val T4 = spark.sql("SELECT * FROM transaction WHERE TransTotal < 600")
		T4.registerTempTable("T4")
		// Over T4, group the transactions by customer ID, 
		// for each group, report the customer ID, and the transactions’ count.
		val T5 = spark.sql("SELECT CustID, COUNT(transID) AS transCount  FROM T4 GROUP BY CustID")
		T5.registerTempTable("T5")
		//Select the customer IDs whose T5.count * 5 < T3.count
		val T6 = spark.sql("SELECT T5.CustID FROM T5, T3 WHERE T5.CustID = T3.CustID AND T5.transCount * 5 < T3.transCount")

		// Report back T6 to the client side
		T6.show()
    }
}
