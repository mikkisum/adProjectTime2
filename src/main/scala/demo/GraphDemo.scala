package demo

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 图计算好友关联推荐
  */

object GraphDemo {
  def main(args: Array[String]): Unit = {
    //第一步创建sparkSession
    val ssc: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local").getOrCreate()
    //创建点和边
      //构建点的集合
    val point: RDD[(Long, (String, Int))] = ssc.sparkContext.makeRDD(Seq(
      (1L, ("大白", 26)),
      (2L, ("大灰", 27)),
      (6L, ("小恶魔", 24)),
      (9L, ("大大", 26)),
      (133L, ("大小", 26)),
      (138L, ("大白白", 26)),
      (16L, ("大黑", 26)),
      (44L, ("大皇", 26)),
      (21L, ("大黄", 26)),
      (5L, ("大荒", 26)),
      (7L, ("大晃", 26))
    ))

      //构建边的集合
    val edge = ssc.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))
      //构建图
    val graph = Graph(point,edge)
      //确定顶点
    val vertices = graph.connectedComponents().vertices
      //匹配数据
    vertices.foreach(println)
      vertices.join(point).map{
        case(userId,(cnId,(name,age)))=>(cnId,List((name,age)))
      }
      .reduceByKey(_++_)
      .foreach(println)



    //构建图
  }

}
