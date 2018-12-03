
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class DailyETLJob(runDay: String) {

  val user2session: String = "select distinct " +
    "sess_id, " +
    "customers_id as user_id " +
    "from recommendation.rec_session " +
    "where dctime = " + "'" + runDay + "' " +
    "and customers_id > 0"

  val sessionClick: String = "select distinct " +
    "sess_id, " +
    "products_id as product_id " +
    "from recommendation.rec_session_action " +
    "where dctime = " + "'" + runDay + "' " +
    "and site in (" +
    "\"m.newchic.com\", \"www.newchic.com\", \"android.newchic.com\"," +
    "\"ios.newchic.com\", \"ar-ios.newchic.com\", \"ar-android.newchic.com\"," +
    "\"ar-m.newchic.com\", \"m.newchic.in\", \"android.newchic.in\"," +
    "\"ios.newchic.in\", \"www.newchic.in\") " +
    "and com = 'view' " +
    "and products_id != 0 " +
    "and r_position rlike '.*(buytogether\\\\-auto|youMayAlsoLike|home_recommend|category|categories|hot_product).*'"

  val sessionWish: String = "select distinct " +
    "sess_id, " +
    "products_id as product_id " +
    "from recommendation.rec_session_operate " +
    "where dctime = " + "'" + runDay + "' " +
    "and site in (" +
    "\"m.newchic.com\", \"www.newchic.com\", \"android.newchic.com\"," +
    "\"ios.newchic.com\", \"ar-ios.newchic.com\", \"ar-android.newchic.com\"," +
    "\"ar-m.newchic.com\", \"m.newchic.in\", \"android.newchic.in\"," +
    "\"ios.newchic.in\", \"www.newchic.in\") " +
    "and operating = 'wish' " +
    "and products_id != 0 " +
    "and r_position rlike '.*(buytogether\\\\-auto|youMayAlsoLike|home_recommend|category|categories|hot_product).*'"

  val sessionCart: String = "select distinct " +
    "sess_id, " +
    "products_id as product_id " +
    "from recommendation.rec_session_basket " +
    "where dctime = " + "'" + runDay + "' " +
    "and site in (" +
    "\"m.newchic.com\", \"www.newchic.com\", \"android.newchic.com\"," +
    "\"ios.newchic.com\", \"ar-ios.newchic.com\", \"ar-android.newchic.com\"," +
    "\"ar-m.newchic.com\", \"m.newchic.in\", \"android.newchic.in\"," +
    "\"ios.newchic.in\", \"www.newchic.in\")" +
    "and operating in ('cart', 'buynow')" +
    "and products_id != 0 " +
    "and r_position rlike '.*(buytogether\\\\-auto|youMayAlsoLike|home_recommend|category|categories|hot_product).*'"

  val sessionOrder: String = "select distinct " +
    "sess_id, " +
    "products_id as product_id " +
    "from recommendation.rec_order_products " +
    "where from_unixtime(add_time, 'yyyy-MM-dd') = " + "'" + runDay + "' " +
    "and site in (" +
    "\"m.newchic.com\", \"www.newchic.com\", \"android.newchic.com\"," +
    "\"ios.newchic.com\", \"ar-ios.newchic.com\", \"ar-android.newchic.com\"," +
    "\"ar-m.newchic.com\", \"m.newchic.in\", \"android.newchic.in\"," +
    "\"ios.newchic.in\", \"www.newchic.in\") " +
    "and orders_status not in (1,4,6,12,17,20,21,22,23,27) " +
    "and products_id != 0 " +
    "and r_position rlike '.*(buytogether\\\\-auto|youMayAlsoLike|home_recommend|category|categories|hot_product).*'"
}

object DailyETLJob {

  def main(args: Array[String]): Unit = {
    var date: String = ""
    if (args.length == 0) {
      date = DateUtil.getDiffDate()
    } else {
      date = args(0)
    }

    val etl: DailyETLJob = new DailyETLJob(date)
    val spark: SparkSession = SparkUtils.getSparkSession("user-event-daily")

    val user2sessionDf: DataFrame = spark.sql(etl.user2session)
    val sessionClickDf: DataFrame = spark.sql(etl.sessionClick)
    val sessionWishDf: DataFrame = spark.sql(etl.sessionWish)
    val sessionCartDf: DataFrame = spark.sql(etl.sessionCart)
    val sessionOrderDf: DataFrame = spark.sql(etl.sessionOrder)

    val joinCols = Seq("sess_id")

    val clickEvents: DataFrame = user2sessionDf.join(
      sessionClickDf, joinCols, "inner"
    ).select("sess_id", "user_id", "product_id"
    ).withColumn("event", lit("click"))

    val wishEvents: DataFrame = user2sessionDf.join(
      sessionWishDf, joinCols, "inner"
    ).select("sess_id", "user_id", "product_id"
    ).withColumn("event", lit("wish"))

    val cartEvents: DataFrame = user2sessionDf.join(
      sessionCartDf, joinCols, "inner"
    ).select("sess_id", "user_id", "product_id"
    ).withColumn("event", lit("cart"))

    val orderEvents: DataFrame = user2sessionDf.join(
      sessionOrderDf, joinCols, "inner"
    ).select("sess_id", "user_id", "product_id"
    ).withColumn("event", lit("order"))

    clickEvents.union(wishEvents
    ).union(cartEvents
    ).union(orderEvents
    ).withColumn("event_uv", lit(1)
    ).withColumn("event_time", lit(date)
    ).write.mode("append").
      format("hive").
      insertInto("ai.nc_customer_daily_events")
  }
}
