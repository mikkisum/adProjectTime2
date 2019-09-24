package rqt

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

object ProCityDistribution {
  def main(args: Array[String]): Unit = {
    //建立连接 分别用sparkcore和sparksql做
    if(args.length!=2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath)=args
    //创建上下文对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
      //设置序列化方式
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建sqlContext
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")


    //读取数据 使用sparksql读取parquet文件
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    //聚合或查询生成结果
    df.registerTempTable("data")
//    val frame: DataFrame = sQLContext.sql("select count(1) as ct," +
//      "provincename, " +
//      "cityname " +
//      "from " +
//      "data " +
//      "group by " +
//      "provincename, "  +
//      "cityname ").cache()
//    //将数据存成json格式
//    frame.show()
//    frame.write.partitionBy("provincename","cityname").json(outputPath)
//    //将数据结果存入mysql
//      //加载配置文件 需要使用对应的依赖
//    val load: Config = ConfigFactory.load()
//    val prop = new Properties()
//    prop.setProperty("user",load.getString("jdbc.user"))
//    prop.setProperty("password",load.getString("jdbc.password"))
//
//    frame.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tablename"),prop)

    //--延伸：使用sparkcore实现并将结果存入磁盘'

    //在使用sparkcore实现
    val rdd: RDD[Row] = sQLContext.sql("select provincename,cityname from data").rdd//这步没有必要，懒得改了
    val unit: RDD[((String, String), Int)] = rdd.mapPartitions(f => {
      f.map(line => (((line.getAs("provincename"), line.getAs("cityname")), 1)))
    })
    val res: RDD[((String, String), Int)] = unit.reduceByKey(_+_)
    println(res.collect().toBuffer)
    //利用repartition算子可以进行重新分区
    res.repartition(2).saveAsTextFile("e://rddResult")


    sc.stop()
  }





}
