import java.io.{File, PrintWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}



object Xuehan_Zhao_task1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("JSON")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val mySchema = (new StructType).add("asin", StringType).add("overall", DoubleType)

//    val df = sqlContext.read.schema(mySchema).json("reviews_Toys_and_Games_5.json")
    val df = sqlContext.read.schema(mySchema).json(args(0))

    val output = df.groupBy("asin").mean().orderBy("asin")

    val tmp = output.collect()
//    val writer = new PrintWriter(new File("Xuehan_Zhao_result_task1.csv" ))
    val writer = new PrintWriter(new File(args(1)))

    writer.write("asin,rating_avg\n")
    tmp.foreach(Row => writer.write("%s,%s\n" format (Row.get(0), Row.get(1))))
    writer.close()
  }
}
