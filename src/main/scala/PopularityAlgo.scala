import helpers.{EventID, ProductID}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time.Interval
import scala.collection.mutable.ListBuffer


object PopularityAlgo {
  private val spark = SparkUtils.getSparkSession("calcPop")

  def main(args: Array[String]): Unit = {
    val endDate: Option[String] = Some(args(0))

    val eventNames = ModelParams.eventNames
    val interval = DateUtil.getTimeInterval(offsetDate = endDate)
    val popEventsMap = calcPopular(eventNames, interval)
    val trendingEventsMap = calcTrending(eventNames, interval)
    val hotEventsMap = calcHot(eventNames, interval)

    val popDf = map2dataFrame("pop", eventNames, popEventsMap)
    val trendDf = map2dataFrame("trend", eventNames, trendingEventsMap)
    val hotDf = map2dataFrame("hot", eventNames, hotEventsMap)

    val comDf = popDf.join(
      trendDf, "product_id"
    ).join(
      hotDf, "product_id"
    )

    print("The final combine df schema: " + comDf.schema)
    comDf.write
      .mode("Overwrite")
      .format("hive")
      .insertInto("ai.nc_item_popularity")
  }

  def calcPopular(
                   eventNames: List[String],
                   interval: Interval): Map[EventID, RDD[(ProductID, Float)]] = {
    val itemEventUVs = DataSource.readingItemEventUVs(interval)
    val event2pop = eventNames.map {
      event => {
        val singleItemEventUVs = itemEventUVs
          .filter(_.event == event)
          .map(obj => (obj.productId, obj.eventUV))
        event -> singleItemEventUVs
      }
    }
    event2pop.toMap
  }

  def calcTrending(
                    eventNames: List[String],
                    interval: Interval): Map[EventID, RDD[(ProductID, Float)]] = {
    var eventTrendingMap: Map[String, RDD[(ProductID, Float)]] = Map()

    val halfInterval = interval.toDurationMillis / 2
    val olderInterval = new Interval(interval.getStart, interval.getStart.plus(halfInterval))
    val newerInterval = new Interval(interval.getStart.plus(halfInterval), interval.getEnd)

    val olderPopMap = calcPopular(eventNames, olderInterval)

    if (olderPopMap.nonEmpty) {
      val newerPopMap= calcPopular(eventNames, newerInterval)
      eventTrendingMap = minusInMap(eventNames, newerPopMap, olderPopMap)
    }
    eventTrendingMap
  }

  def calcHot(
               eventNames: List[String] = List.empty,
               interval: Interval): Map[EventID, RDD[(ProductID, Float)]] = {
    var eventHotMap: Map[String, RDD[(ProductID, Float)]] = Map()

    val olderInterval = new Interval(interval.getStart, interval.getStart.plus(interval.toDurationMillis / 3))
    val middleInterval = new Interval(olderInterval.getEnd, olderInterval.getEnd.plus(olderInterval.toDurationMillis))
    val newerInterval = new Interval(middleInterval.getEnd, interval.getEnd)

    val olderPopMap = calcPopular(eventNames, olderInterval)
    if (olderPopMap.nonEmpty) {
      val middlePopMap = calcPopular(eventNames, middleInterval)
      if (middlePopMap.nonEmpty) {
        val newerPopMap = calcPopular(eventNames, newerInterval)
        val newVelocityRDD = minusInMap(eventNames, newerPopMap, middlePopMap)
        val oldVelocityRDD = minusInMap(eventNames, middlePopMap, olderPopMap)
        eventHotMap = minusInMap(eventNames, newVelocityRDD, oldVelocityRDD)
      }
    }
    eventHotMap
  }

  private def minusInMap(
                          eventNames: List[String],
                          newerMap: Map[EventID, RDD[(ProductID, Float)]],
                          olderMap:Map[EventID, RDD[(ProductID, Float)]]
                        ): Map[EventID, RDD[(ProductID, Float)]] = {
    var eventsMap: Map[String, RDD[(ProductID, Float)]] = Map()
    eventNames.foreach {
      event => {
        val oldSinglePopRdd = olderMap(event)
        val newSinglePopRdd = newerMap(event)
        val singleTrendRdd = newSinglePopRdd.join(oldSinglePopRdd).map {
          case (item, (newerScore, olderScore)) => item -> (newerScore - olderScore)
        }
        eventsMap += (event -> singleTrendRdd)
      }
    }
    eventsMap
  }

  private def map2dataFrame(
                             theme: String,
                             eventNames: List[String],
                             eventsMap: Map[String, RDD[(ProductID, Float)]]): DataFrame = {
    val cols = new ListBuffer[String]()
    val dfList: List[DataFrame] = eventNames.map {
      event => {
        val singleRdd: RDD[(ProductID, Float)] = eventsMap(event)
        val col = theme + "_" + event
        cols += col
        spark.createDataFrame(singleRdd).toDF("product_id", col)
      }
    }
    var headDf = dfList.head
    dfList.tail.foreach {
      rightDf =>
        headDf = headDf.join(rightDf, Seq("product_id"), "fullouter")
    }
    headDf.na.fill(0.0, cols)
  }
}
