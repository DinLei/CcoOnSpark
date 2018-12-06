import helpers.{EventID, ProductID, UserID}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.joda.time.Interval

class DataSource(
                  minEventsPerUser: Option[Int],
                  startDate: String, endDate: String
                ) extends Serializable {
  private val eventNames = ModelParams.eventNames

  def readingTraining(): TrainingData = {
    val eventsDs: Dataset[Events] = DataSource.readingRawEventsData(
      startDate, endDate
    )

    import DataSource.spark.implicits._
    val eventRDDs: List[(EventID, RDD[(UserID, ProductID)])] = eventNames.map {
      eventName =>
        val singleEventRDD: RDD[(UserID, ProductID)] = eventsDs.filter(
          record => record.event == eventName
        ).map(
          record => (record.userId, record.productId)
        ).rdd
        (eventName, singleEventRDD)
    } filterNot { case (_, singleEventRDD) => singleEventRDD.isEmpty() }

    TrainingData(eventRDDs, minEventsPerUser)
  }
}

object DataSource {
  private val spark = SparkUtils.getSparkSession("get-training-data")

  def readingRawEventsData(
                            startDate: String,
                            endDate: String
                          ): Dataset[Events]= {
    val eventsHql: String = "select " +
      "event, " +
      "user_id, " +
      "product_id, " +
      "count(event_uv) as event_uvs " +
      "from ai.nc_customer_daily_events " +
      "where event_time >= '" + startDate + "' " +
      "  and event_time <= '" + endDate + "' " +
      "group by user_id, product_id, event " +
      "order by event, user_id, product_id"
    import spark.implicits._
    spark.sql(eventsHql).map(
      row => {
        val event: EventID = row.get(0).toString
        val userId: UserID = row.get(1).toString
        val productId: ProductID = row.get(2).toString
        Events(event, userId, productId)
      }
    )
  }

  def readingItemEventUVs(interval: Interval):RDD[ItemEventUVs] = {
    val startDate = interval.getStart.toString(DateUtil.dateFormatStr)
    val endDate = interval.getEnd.toString(DateUtil.dateFormatStr)
    val hql = "SELECT " +
      "event, " +
      "product_id, " +
      "cast(sum(event_uv) as float) AS event_uv " +
      "FROM ai.nc_customer_daily_events " +
      "WHERE event_time >= '" + startDate + "' " +
      "AND event_time <= '" + endDate + "' " +
      "GROUP BY event, product_id"
    import spark.implicits._
    spark.sql(hql).map(
      row => {
        val event: EventID = row.get(0).toString
        val productId: ProductID = row.get(1).toString
        val eventUVs: Float = row.getFloat(2)
        ItemEventUVs(event, productId, eventUVs)
      }
    ).rdd
  }

  def readingItemDesc(): DataFrame = {
    val hql = "SELECT " +
      "pd.products_id AS product_id, " +
      "pd.products_name AS product_name, " +
      "ptc.cat_path AS cat_path " +
      "FROM newchic.products_description pd " +
      "JOIN newchic.products_to_categories ptc " +
      "ON pd.products_id = ptc.products_id"

    import spark.implicits._
    spark.sql(hql).map(
      row => {
        val productId: ProductID = row.get(0).toString
        val text: Array[String] = row.get(1).toString.toLowerCase.split(" ")
        val catPath: String = row.get(2).toString
        val catList: Array[String] = catPath.split("-")
        val cat4cat: String = if (catList.length >= 2) catList(1) else catList(0)
        (productId, catPath, cat4cat, text)
      }
    ).toDF("product_id", "cat_path", "cat4cat", "text")
  }
}






