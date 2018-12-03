
import org.apache.mahout.math.RandomAccessSparseVector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {
  val appName: String = "nc-universal-rec"
  /*
  val configEnv: Config = ConfigFactory.load("events.conf")

  val tableMaster: Option[String] = Some(configEnv.getString(s"${appName}.table-master"))
  val colFamily: Option[String] = Some(configEnv.getString(s"${appName}.column-family"))
  val hBaseHost: Option[String] = Some(configEnv.getString(s"${appName}.hbase-db-host"))
  val hBasTable: Option[String] = Some(configEnv.getString(s"${appName}.hbase-table"))
  val hiveDataWarehouse: Option[String] = Some(configEnv.getString(s"${appName}.hive-data-warehouse"))

  val defaultPartitions: Option[Int] = Some(configEnv.getInt(s"${appName}.defaultPartitions"))
  val ccoModelSavePath: Option[String] = Some(configEnv.getString(s"${appName}.ccoModelSavePath"))
  */

  val hiveDataWarehouse: Option[String] = Some("hdfs://ns1/apps/hive/warehouse/ai.db")
  val defaultPartitions: Option[Int] = Some(250)
  val ccoModelSavePath: Option[String] = Some("hdfs://ns1/apps/hive/warehouse/ai.db/nc_cco/")


  def getSparkSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.warehouse.dir", hiveDataWarehouse.get)
      .config("spark.sql.shuffle.partitions", defaultPartitions.get)
      .config("spark.driver.maxResultSize", "10g")
      .enableHiveSupport()
      .getOrCreate()
  }

  def getSparkContext(appName: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    conf.registerKryoClasses(
      Array(
        classOf[RandomAccessSparseVector]
      )
    )
    new SparkContext(conf)
  }

  def readDataFromHive(hql:String): DataFrame = {
    val spark: SparkSession = SparkUtils.getSparkSession("sparkOnHive")
    spark.sql(hql)
  }

  def readTrainingFromHBase: DataFrame = {
    null
  }

  def writeDfToHBase(dataFrame: DataFrame): Unit = {
    
  }
}