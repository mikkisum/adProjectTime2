package etl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import util.{SchemaUtil, String2Type}

object Log2parquet {
  def main(args: Array[String]): Unit = {
    //指定Hadoop路径
    //System.setProperty("hadoop.home.dir","E:\\software\\hadoop\\hadoop-2.7.2")
    //判断路径是否正确
    if(args.length!=2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPathLocal)=args
    //创建上下文对象
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName(this.getClass.getName)
    //设置序列化方式
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建sqlContext
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    //读取
    val lines: RDD[String] = sc.textFile(inputPath)
    //处理
    val data: RDD[Array[String]] = lines.map(t=>t.split(",",t.length)).filter(_.length==85)
    val rowRDD: RDD[Row] = data.map(arr => {
      Row(
        arr(0),
        String2Type.toInt(arr(1)),
        String2Type.toInt(arr(2)),
        String2Type.toInt(arr(3)),
        String2Type.toInt(arr(4)),
        arr(5),
        arr(6),
        String2Type.toInt(arr(7)),
        String2Type.toInt(arr(8)),
        String2Type.toDouble(arr(9)),
        String2Type.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        String2Type.toInt(arr(17)),
        arr(18),
        arr(19),
        String2Type.toInt(arr(20)),
        String2Type.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        String2Type.toInt(arr(26)),
        arr(27),
        String2Type.toInt(arr(28)),
        arr(29),
        String2Type.toInt(arr(30)),
        String2Type.toInt(arr(31)),
        String2Type.toInt(arr(32)),
        arr(33),
        String2Type.toInt(arr(34)),
        String2Type.toInt(arr(35)),
        String2Type.toInt(arr(36)),
        arr(37),
        String2Type.toInt(arr(38)),
        String2Type.toInt(arr(39)),
        String2Type.toDouble(arr(40)),
        String2Type.toDouble(arr(41)),
        String2Type.toInt(arr(42)),
        arr(43),
        String2Type.toDouble(arr(44)),
        String2Type.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        String2Type.toInt(arr(57)),
        String2Type.toDouble(arr(58)),
        String2Type.toInt(arr(59)),
        String2Type.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        String2Type.toInt(arr(73)),
        String2Type.toDouble(arr(74)),
        String2Type.toDouble(arr(75)),
        String2Type.toDouble(arr(76)),
        String2Type.toDouble(arr(77)),
        String2Type.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        String2Type.toInt(arr(84))
      )
    })
    rowRDD

    val df: DataFrame = sQLContext.createDataFrame(rowRDD,SchemaUtil.structType)
    //写入
      //df.write.partitionBy("provincename","cityname").parquet(outputPathLocal)
      df.write.parquet(outputPathLocal)
      //写入到hdfs//指定链接时需要指定存活的那台节点
      //df.write.partitionBy("provincename","cityname").mode(SaveMode.Overwrite).parquet(outputPathHDFS)












  }


}
