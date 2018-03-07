import java.io.{File, PrintWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object Xuehan_Zhao_task2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("JSON")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val mySchema = (new StructType).add("asin", StringType).add("overall", DoubleType)
    val mySchema2 = (new StructType).add("asin", StringType).add("brand", StringType)


    //val reviewData = sqlContext.read.schema(mySchema).json("reviews_Toys_and_Games_5.json")
    val reviewData = sqlContext.read.schema(mySchema).json(args(0))
   // val metaData = sqlContext.read.schema(mySchema2).json("metadata.json")
   val metaData = sqlContext.read.schema(mySchema2).json(args(1))

    val filteredMetaData = metaData.filter(metaData("brand") =!= null || metaData("brand") =!= "")

    val joinedData = reviewData
      .join(filteredMetaData, reviewData("asin") === filteredMetaData("asin"), "inner")
      .drop(filteredMetaData("asin"))


    val output = joinedData.drop("asin").groupBy("brand").mean().orderBy("brand")

    //val outputRename = Seq("brand", "rating_avg")
    //val newOutput = output.toDF(outputRename: _*)

    val tmp = output.collect()
    //val writer = new PrintWriter(new File("Xuehan_Zhao_result_task2.csv" ))
    val writer = new PrintWriter(new File(args(2)))
    writer.write("brand,rating_avg\n")
    tmp.foreach(Row => writer.write("%s,%s\n" format (Row.get(0), Row.get(1))))
    writer.close()

  }
}
