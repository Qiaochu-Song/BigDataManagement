//import spark.implicits._
import scala.math._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

case class Point(x:Int, y: Int)
//case class cell_RD(cell_idx:Int, relative_density: Double)
//case class top_neighbors(cell_idx:Int, neighbor_idx:Int, density:Double)
object SparkQ3 extends Serializable {
    def main(args: Array[String]): Unit = {  // args = <dir/to/point.txt> <outputDir>
		val spark = SparkSession.builder().appName("question3").config("spark.some.config", "some-value").getOrCreate()
        //"/home/mqp/Desktop/points_small.txt"
		val pointsDF =  sc.textFile(args(0)).map(_.split(",")).map(attributes => Point(attributes(0).trim.toInt,attributes(1).trim.toInt))
        //val pointsDF = pointsDF.parallelize(data, 10)

        // grid-cells, each of size 20x20. zero-based, 25000 cells in total
        // calculate #points for each cell. MR output as (cell_idx, #points) 
        val num_points = pointsDF.map(p => (floor(p.x/20).toInt + (500 - ceil(p.y/20)).toInt * 500, 1)).reduceByKey(_ + _) // key = grid index
        // find neighbors for each cell, output (cell_idx, neighbor_idx_n)
        val num_neighbors = num_points.flatMap(p => neighbors(p._1))
        // join num_points with num_neighbors to get (cell_idx, neighbor_idx_n, #points_of_this_neighbor)
        val manipulated_num_points = num_points.keyBy(t => t._1)  // join with cell_idx
        val manipulated_num_neighbors = num_neighbors.keyBy(t => t._2)  // join with neighbor_idx
        
        // join and format as (cell_idx, neigbor_idx, #neighbor_points), groupBy cell_idx
        // val joint = manipulated_num_neighbors.join(manipulated_num_points).map(t => (t._1._1, t._1._2, t._2._2)).keyBy(t =>t._1) // keyBy can be omitted
        // join and format as (cell_idx, #neighbor_points), groupBy cell_idx
        val joint = manipulated_num_neighbors.join(manipulated_num_points).map(t => (t._2._1._1, t._2._2._2))
        // (neighbor_idx, ((cell_idx, neighbor_idx),(neighbor_idx, #neighbor's_points))) => (cell_idx, #neighbor's_points)

        // compute sum_neighbor_points, output = (cell_idx, sum_neighbor_points)
        val sum_points = joint.reduceByKey((p1, p2) => p1 + p2)
        // compute avg(neighbor_points), output = (cell_idx, neighbor_density)
        val neighbor_density = sum_points.map(p => (p._1, p._2.toDouble / count_neighbors(p._1)))
        // result = (cell_idx, relative_density)
        val result = num_points.join(neighbor_density).map(p => (p._1, p._2._1 / p._2._2))
        
        val top = result.sortBy(_._2, false)  // false indicating descending order, use take() to get top 50

        val top_50 = top.toDF().limit(50)
        top_50.show()

        // question 2

        val top_neighbors = top_50.flatMap(p => neighbors(p.getInt(0))).map(p=>(p._2,p._1))  // get neighbors of top 50 grid-cells
        // the key is neighbors_idx, value is top_cell_idx
        // join this with the "num_points", output: (neighbor_idx,(top_cell_idx, #neighbor_points))
        //val top_neighbors_points = top_neighbors.join(num_points).map(p => (p.getInt(1),p.getInt(0),p.getInt(2)))
        // reorder as : (top_cell_idx, neighbor_idx, #relative_density_of_this_neighbor)
        val top_neighbors_points = top_neighbors.rdd.join(num_points).map(p => (p._2._1, p._1, p._2._2))
        top_neighbors_points.toDF().show()
        }

    // helper: return neighbor index of a cell
    // call this method when mapping one cell to its neighbors
    // return an array of zipped pairs: Array((cell_idx, neighbor_idx1), (cell_idx, neighbor_idx2), ...)
    def neighbors(cell_idx: Int): Array[(Int, Int)] = {
    	val neighbors = ArrayBuffer[Int]()
    	if (cell_idx >= 500){ neighbors += (cell_idx - 500) }  // has upper neighbor
    	if ((cell_idx) % 500 != 0) { neighbors += (cell_idx - 1) }  // has left neighbor
    	if ((cell_idx + 1) % 500 != 0) { neighbors += (cell_idx + 1) }  // has right neighbor
    	if (cell_idx  < 24500) { neighbors += (cell_idx + 500) }  // has bottom neighbor

    	if (cell_idx >= 500 && (cell_idx) % 500 != 0) {neighbors += (cell_idx - 501)}  // has upper left neighbor
        if (cell_idx >= 500 && (cell_idx + 1) % 500 != 0) {neighbors += (cell_idx - 499)}  // has upper right neighbor
        if (cell_idx  < 24500 && (cell_idx) % 500 != 0) {neighbors += (cell_idx + 499)}  // has bottom left neighbor
        if (cell_idx + 1 < 24500  && (cell_idx + 1) % 500 != 0) {neighbors += (cell_idx + 501)}  // has bottom right neighbor
        neighbors.toArray

        return Array.fill[Int](neighbors.size)(cell_idx) zip neighbors
        //return neighbors.toArray
        }

    // helper: return #neighbors of a cell
    def count_neighbors(cell_idx: Int): Int = {
    	var count: Int = 0
    	if (cell_idx >= 500){ count +=1 }  // has upper neighbor
    	if ((cell_idx) % 500 != 0) { count +=1 }  // has left neighbor
    	if ((cell_idx + 1) % 500 != 0) { count +=1 }  // has right neighbor
    	if (cell_idx  < 24500) { count +=1 }  // has bottom neighbor

    	if (cell_idx >= 500 && (cell_idx) % 500 != 0) {count+=1 }  // has upper left neighbor
        if (cell_idx >= 500 && (cell_idx + 1) % 500 != 0) {count+=1 }  // has upper right neighbor
        if (cell_idx < 24500 && (cell_idx) % 500 != 0) {count+=1 }  // has bottom left neighbor
        if (cell_idx < 24500 && (cell_idx + 1) % 500 != 0) {count+=1 }  // has bottom right neighbor
        assert(count>0)
        return count
        }


    }