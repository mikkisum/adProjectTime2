package label

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import util.{AdLocationTagMaker, AppTagMaker, BusinessTagMaker, DeviceTag, KeywordTagMaker, TagIdHelper}

object AllLabel {
  def main(args: Array[String]): Unit = {
    val log: Log = LogFactory.getLog(this.getClass)
    //建立连接
    if (args.length != 4) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath, outputPath, dicdir, stopwords) = args
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
      //设置序列化方式
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建sqlContext
    //val sc = new SparkContext(conf)
    //val ssc: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val ssc: SparkSession = SparkSession.builder()
      .config("spark.speculation", true)
      .config("spark.speculation.interval", 1000)
      .config("spark.speculation.multiplier", 1.5)
      .config("spark.speculation.quantile", 0.10)
      .config(conf)
      .getOrCreate()

    //调用HbaseAPI
    val load: Config = ConfigFactory.load()
    //获取表名
    val HbaseTableName: String = load.getString("HBASE.tableName")
    //创建Hadoop任务
    val configuration: Configuration = ssc.sparkContext.hadoopConfiguration
    //配置Hbase连接
    configuration.set("hbase.zookeeper.quorum", load.getString("HBASE.Host"))
    //获取connection连接
    val hbConn: Connection = ConnectionFactory.createConnection(configuration)
    val hadmin: Admin = hbConn.getAdmin
    //判断当前表是否被使用
    if (!hadmin.tableExists(TableName.valueOf(HbaseTableName))) {
      println("当前表可用")
      //创建表对象
      val hbaseTableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
      //创建列簇
      val hColumnDescriptor = new HColumnDescriptor("tags")
      //将创建好的列簇加入表中
      hbaseTableDescriptor.addFamily(hColumnDescriptor)
      hadmin.createTable(hbaseTableDescriptor)
    }
    hadmin.close()
    hbConn.close()

    val hadoopConf = new JobConf(configuration)
    //指定输出类型
    hadoopConf.setOutputFormat(classOf[TableOutputFormat])
    //指定输出到那张表
    hadoopConf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)


    //读取数据 使用sparksql读取parquet文件
    val df: DataFrame = ssc.read.parquet(inputPath) //E:\gitPro\adProjectTime2\doc\app_dict.txt
    //获取app标签需要用到的字典文件
    import ssc.implicits._
    val info: RDD[String] = ssc.sparkContext.textFile(dicdir)
    val flitered: RDD[Array[String]] = info.map(line => line.split("\t", -1)).filter(_.length >= 4)

    //读取停用词库文件
    val stopwordRdd: RDD[String] = ssc.sparkContext.textFile(stopwords)
    val stopwordTupRdd: RDD[(String, Int)] = stopwordRdd.map((_, 1))
    val stwBC: Broadcast[collection.Map[String, Int]] = ssc.sparkContext.broadcast(stopwordTupRdd.collectAsMap())

    val dic: RDD[(String, String)] = flitered.map(arr => (arr(4), arr(1)))
    //广播
    val broadcast: Broadcast[collection.Map[String, String]] = ssc.sparkContext.broadcast(dic.collectAsMap())
    //处理拼接
    val filtered: DataFrame = df.filter(TagIdHelper.userIdFliterCondition)


    val res: RDD[(String, List[(String, Int)])] = filtered.rdd.map(row => {
      //获取userid
      val anyoneId: String = TagIdHelper.getAnyoneLegalId(row)
      val allIdList: List[String] = TagIdHelper.getAllLegalId(row)
      //打标签
      var list = List[(String, Int)]()
      val asLocationTag: List[(String, Int)] = AdLocationTagMaker.makeTag(row)
      val appTag: List[(String, Int)] = AppTagMaker.makeTag(row, broadcast)
      val keywordTag: List[(String, Int)] = KeywordTagMaker.makeTag(row, stwBC)
      val deviceTag: List[(String, Int)] = DeviceTag.makeTag(row)

      log.info("AllLabel_ready to make businessTags")
      //val businessTagMaker: List[(String, Int)] = BusinessTagMaker.makeTag(row)
      log.info("AllLabel_sucessfully made businessTags")


      //组合
      (anyoneId, asLocationTag ::: appTag ::: keywordTag ::: deviceTag)

      //(anyoneId, businessTagMaker)

    })
    val resReduced: RDD[(String, List[(String, Int)])] = res.reduceByKey((list1, list2) =>
      (list1 ::: list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft(0)(_ + _._2)).toList)
    resReduced.collect().toBuffer.foreach(println)

    //将聚合后的结果存储到hbase中,需要先做处理，变成能够存储到hbase中的格式
    //(uid,List())
    resReduced.map {
      case (userId, userTags) => {
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes("20190923"), Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(), put)
      }
    } //为什么要用大括号呢
        .saveAsHadoopDataset(hadoopConf)


    res.collect().toBuffer.foreach(println)

    //关闭连接
    ssc.stop()
  }

}
