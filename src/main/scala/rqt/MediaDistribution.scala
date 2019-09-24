package rqt


import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.tools.scalap.scalax.util.StringUtil



object MediaDistribution {
  def main(args: Array[String]): Unit = {
    if(args.length!=3){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath,dicdir)=args
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
      //设置序列化方式
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建sqlContext
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    //处理字典文件
   val flitered: RDD[Array[String]] = sc.textFile(dicdir).map(line=>line.split("\t",-1)).filter(_.length>=4)
    val dic: RDD[(String, String)] = flitered.map(arr=>(arr(4),arr(1)))
    //广播
    val broadcast: Broadcast[collection.Map[String, String]] = sc.broadcast(dic.collectAsMap())


    //读取数据 使用sparksql读取parquet文件
    val df: DataFrame = sQLContext.read.parquet(inputPath)


    //处理
    val rdd: RDD[Row] = df.rdd
    val value: RDD[(String, List[Double])] = rdd.mapPartitions(f => {
      f.map(line => {
        val appid: String = line.getAs[String]("appid")
        var appname: String = line.getAs[String]("appname")
        val requestmode: Int = line.getAs[Int]("requestmode")
        val processmode: Int = line.getAs[Int]("processnode")
        val iseffective: Int = line.getAs[Int]("iseffective")
        val isbilling: Int = line.getAs[Int]("isbilling")
        val isbid: Int = line.getAs[Int]("isbid")
        val iswin: Int = line.getAs[Int]("iswin")
        val adorderid: Int = line.getAs[Int]("adorderid")
        val winprice: Double = line.getAs[Double]("winprice")
        val adpayment: Double = line.getAs[Double]("adpayment")

        //判断
        //创建一个list用来存放结果
        val list =
        List[Double](if (requestmode == 1 && processmode >= 1) 1 else 0) ++
          List[Double](if (requestmode == 1 && processmode >= 2) 1 else 0) ++
          List[Double](if (requestmode == 1 && processmode == 3) 1 else 0) ++
          List[Double](if (iseffective == 1 && isbilling == 1 && isbid == 1) 1 else 0) ++
          List[Double](if (iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0) 1 else 0) ++
          List[Double](if (requestmode == 2 && iseffective == 1) 1 else 0) ++
          List[Double](if (requestmode == 3 && iseffective == 1) 1 else 0) ++
          List[Double](if (iseffective == 1 && isbilling == 1 && iswin == 1) winprice / 1000 else 0) ++
          List[Double](if (iseffective == 1 && isbilling == 1 && iswin == 1) adpayment / 1000 else 0)
        //返回list
        //println((tuple, list))
        //在这里匹配字典文件
        val map: collection.Map[String, String] = broadcast.value //(url,appname)
        if (StringUtils.isBlank(appname)) {
          appname = map.getOrElse(appid, appid)
        }
        else appname

        (appname, list)
      })
    })


    value.reduceByKey((list1, list2) => list1.zip(list2).map(t => t._1 + t._2)).collect().foreach(println)

    value.reduceByKey((list1, list2) => list1.zip(list2).map(t => t._1 + t._2)).repartition(2)saveAsTextFile(outputPath)

  }


}
