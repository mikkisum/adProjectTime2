package rqt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object DistrictDistribution {
  //获取所需字段

  def main(args: Array[String]): Unit = {
    if(args.length!=3){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath,docs)=args
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
      //设置序列化方式
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建sqlContext
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    //读取并处理字典文件



    //读取数据 使用sparksql读取parquet文件
    val df: DataFrame = sQLContext.read.parquet(inputPath)//E:\gitPro\adProjectTime2\doc\app_dict.txt


    //处理
    val rdd: RDD[Row] = df.rdd
    val value: RDD[((String, String), List[Double])] = rdd.mapPartitions(f => {
      f.map(line => {
        val provincename: String = line.getAs[String]("provincename")
        val cityname: String = line.getAs[String]("cityname")
        val requestmode: Int = line.getAs[Int]("requestmode")
        val processmode: Int = line.getAs[Int]("processnode")
        val iseffective: Int = line.getAs[Int]("iseffective")
        val isbilling: Int = line.getAs[Int]("isbilling")
        val isbid: Int = line.getAs[Int]("isbid")
        val iswin: Int = line.getAs[Int]("iswin")
        val adorderid: Int = line.getAs[Int]("adorderid")
        val winprice: Double = line.getAs[Double]("winprice")
        val adpayment: Double = line.getAs[Double]("adpayment")
        //省市
        val tuple = (provincename, cityname)
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
        (tuple,list)
      })
    })

    //println(value.reduceByKey((list1, list2) => list1.zip(list2).map(t => t._1 + t._2)).collect().toBuffer)

    value.reduceByKey((list1, list2) => list1.zip(list2).map(t => t._1 + t._2)).repartition(2)saveAsTextFile(outputPath)

  }

}
