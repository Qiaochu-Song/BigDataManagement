//import spark.implicits._
import org.apache.spark.sql.SparkSession

//schema
case class M(row: Int, col: Int, value: Int)

object SparkQ4 {
    def main(args: Array[String]): Unit = {  // args = <dir/to/M1.txt> <dir/to/M2.txt> <outputDir>
		val spark = SparkSession.builder().appName("question4").config("spark.some.config", "some-value").getOrCreate()

// *****replace the directory with the directory to M1*****
		val m1DF =  spark.sparkContext.textFile(args(0)).map(_.split(",")).map(attributes => M(attributes(0).trim.toInt,attributes(1).trim.toInt, attributes(2).trim.toInt)).toDF()
		m1DF.registerTempTable("m1")
// *****replace the directory with the directory to M2*****
		val m2DF =  spark.sparkContext.textFile(args(1)).map(_.split(",")).map(attributes => M(attributes(0).trim.toInt,attributes(1).trim.toInt, attributes(2).trim.toInt)).toDF()
		m2DF.registerTempTable("m2")

// the 1st MR job should map/join (i,k,v1) and (k,j,v2) together and do multiplication
// output (i,j,v1*v2)
// the 2nd round should sum up intermediate values with the same index
		val T = m1DF.join(m2DF, m1DF("col")===m2DF("row")).withColumn("temp",m1DF("value")*m2DF("value")).groupBy(m1DF("row"), m2DF("col")).agg(sum("temp") ).orderBy("row","col")

// *****replace the directory to the output directory*****
		T.repartition(1).write.format("com.databricks.spark.csv").save(args(2))  
    }
}