package algorithm

import org.apache.spark.{SparkContext,SparkConf}

/**
 * Created by fansy on 2017/11/15.
 */
object WordCount {
  def main(args: Array[String]) {
    if(args.length!=2){
      System.exit(-1)
    }

    val input = args(0)
    val output = args(1)

    val sc = new SparkContext(new SparkConf())
    sc.textFile(input).flatMap(_.split(" ")).map(x => (x,1)).reduceByKey((x,y) => x +y).saveAsTextFile(output)
    sc.stop()
  }
}
