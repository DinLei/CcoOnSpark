import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, Interval}

import helpers.{EventID, ProductID, UserID}
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark

import scala.collection.JavaConversions._
import org.apache.mahout.sparkbindings._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

package object helpers {
  type UserID = String
  type ProductID = String
  type EventID = String

  implicit class IndexDatasetConversions(
                                          val indexedDataset: IndexedDatasetSpark
                                        )(implicit sc: SparkContext) {
    def toPayloadCorrRDD(eventName: EventID): RDD[itemEventCorr] = {
      val rowIDReverseDictionary = indexedDataset.rowIDs.inverse
      val rowIDReverseDictionaryBcast = sc.broadcast(rowIDReverseDictionary)

      val colIDReverseDictionary = indexedDataset.columnIDs.inverse
      val colIDReverseDictionaryBcast = sc.broadcast(colIDReverseDictionary)

      println("We prepare to rebuild payload format!")

      indexedDataset.matrix.rdd.map[itemEventCorr] {
        case (rowNum, itemVector) =>
          val itemID = rowIDReverseDictionaryBcast.value.getOrElse(rowNum, "INVALID_ITEM_ID")

          val vectorIter: List[String] = itemVector.nonZeroes.map {
            element => (element.index(), element.get())
          }.toList.sortBy(element => -element._2) map {
            item =>
              val subItemID = colIDReverseDictionaryBcast.value.getOrElse(item._1, "-1")
              val corrScore = item._2.formatted("%.5f")
              subItemID + "|" + corrScore
          }
          val vectorString: String = vectorIter.mkString(" ")

          itemEventCorr(itemID, vectorString, eventName)
      }
    }
  }
}


object DateUtil {
  val dateFormatStr: String = "yyyy-MM-dd"
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def getDiffDate(
                   date: Option[String] = None,
                   diff: Option[Int] = Some(1)): String = {
    val diffVal = diff.get
    var runDate: Date = new Date()
    if (date.nonEmpty) {
      runDate = dateFormat.parse(date.get)
    }

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(runDate)
    cal.add(Calendar.DATE, -1*diffVal)
    dateFormat.format(cal.getTime())
  }

  def getNowDate(): String = {
    dateFormat.format(Calendar.getInstance().getTime())
  }

  def getTimeInterval(duration: Option[Int] = None,
                      offsetDate: Option[String] = None): Interval = {
    val end = if (offsetDate.isEmpty) DateTime.now else {
      try {
        ISODateTimeFormat.dateTimeParser().parseDateTime(offsetDate.get)
      } catch {
        case e: IllegalArgumentException => DateTime.now
      }
    }
    new Interval(end.minusDays(duration.getOrElse(ItemPopParams.interval)), end)
  }

  def main(args: Array[String]): Unit = {
    val interval = DateUtil.getTimeInterval(Some(10), Some("2018-11-20"))

    val halfInterval = interval.toDurationMillis / 2
    val olderInterval = new Interval(interval.getStart, interval.getStart.plus(halfInterval))
    val newerInterval = new Interval(interval.getStart.plus(halfInterval), interval.getEnd)

    print(halfInterval, olderInterval, newerInterval)
  }
}

case class itemEventCorr(
                          val productID: ProductID,
                          val payloadCorrScore: String,
                          val eventName: EventID
                        ) extends Serializable


case class TrainingData(eventRDDs: Seq[(EventID, RDD[(UserID, ProductID)])],
                        minEventsPerUser: Option[Int] = Some(1)
                       ) extends Serializable

case class Events(
                   event: EventID,
                   userId: UserID,
                   productId: ProductID
                 ) extends Serializable

case class ItemEventUVs(
                   event: EventID,
                   productId: ProductID,
                   eventUV: Float
                 ) extends Serializable
