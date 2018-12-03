import helpers.{EventID, ProductID, UserID}
import org.apache.mahout.math.RandomAccessSparseVector
import org.apache.mahout.math.indexeddataset.{BiDictionary, IndexedDataset}
import org.apache.mahout.sparkbindings.{DrmRdd, drmWrap}
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Preparator extends Serializable {
  def prepare(
               trainingData: TrainingData
             )(implicit sc: SparkContext): PreparedData = {
    var userDictionary: Option[BiDictionary] = None

    val indexedDatasets = trainingData.eventRDDs.map {
      case (eventName, eventRDD) =>
        val ids = if (
          eventName == trainingData.eventRDDs.head._1 &&
            trainingData.minEventsPerUser.nonEmpty
        ) {
          val dIDS = IndexedDatasetSpark(
            eventRDD, trainingData.minEventsPerUser.get
          )(sc)
          val ddIDS = IndexedDatasetSpark(eventRDD, Some(dIDS.rowIDs))(sc)
          userDictionary = Some(ddIDS.rowIDs)
          ddIDS
        } else {
          val dIDS = IndexedDatasetSpark(eventRDD, userDictionary)(sc)
          userDictionary = Some(dIDS.rowIDs)
          dIDS
        }
        (eventName, ids)
    }
    println("We have rebuilded the training data successfully!")
    PreparedData(indexedDatasets)
  }
}

case class PreparedData(
                         events: Seq[(EventID, IndexedDataset)]
                       ) extends Serializable

object IndexedDatasetSpark {

  def apply(
             elements: RDD[(String, String)],
             minEventsPerUser: Int
           )(implicit sc: SparkContext): IndexedDatasetSpark = {
    val rowIDDictionary = new BiDictionary(
      elements.map {case (rowID, _) => rowID }.distinct().collect()
    )
    val rowIDDictionaryBcast = sc.broadcast(rowIDDictionary)
    val fillteredElements = elements.filter{
      case (rowID, _) => rowIDDictionaryBcast.value.contains(rowID)
    }

    val colIDs = fillteredElements.map {
      case (_, colID) => colID
    }.distinct().collect()
    val colIDDictionary = new BiDictionary(keys = colIDs)
    val colIDDictionaryBcast = sc.broadcast(colIDDictionary)

    val ncol = colIDDictionary.size
    val minEvents = minEventsPerUser

    val downsampledInteractions = elements.groupByKey().filter {
      case (userId, products) => products.size >= minEvents
    }

    val downsampledUserIDDictionary = new BiDictionary(
      downsampledInteractions.map {
        case (userId, _) => userId
      }.distinct().collect()
    )
    val downsampledUserIDDictionaryBcast = sc.broadcast(downsampledUserIDDictionary)

    val interactions = downsampledInteractions.map {
      case (userId, products) =>
        val userKey = downsampledUserIDDictionaryBcast.value.get(userId).get
        val vector = new RandomAccessSparseVector(ncol)
        for (productId <- products) {
          vector.setQuick(colIDDictionaryBcast.value.get(productId).get, 1.0)
        }
        userKey -> vector
    }.asInstanceOf[DrmRdd[Int]].repartition(sc.defaultMinPartitions)

    val drmInteractions = drmWrap[Int](interactions)
    new IndexedDatasetSpark(
      drmInteractions.newRowCardinality(rowIDDictionary.size),
      downsampledUserIDDictionary,
      colIDDictionary
    )
  }

  def apply(
             elements: RDD[(String, String)],
             existingRowIDs: Option[BiDictionary]
           )(implicit sc: SparkContext): IndexedDatasetSpark = {
    val (filteredElements, rowIDDictionaryBcast, rowIDDictionary) = if (existingRowIDs.isEmpty) {
      val newRowIDDictionary = new BiDictionary(elements.map { case (rowID, _) => rowID }.distinct().collect())
      val newRowIDDictionaryBcast = sc.broadcast(newRowIDDictionary)
      (elements, newRowIDDictionaryBcast, newRowIDDictionary)
    } else {
      val existingRowIDDictionaryBcast = sc.broadcast(existingRowIDs.get)
      val elementsRDD = elements.filter {
        case (rowID, _) =>
          existingRowIDDictionaryBcast.value.contains(rowID)
      }
      (elementsRDD, existingRowIDDictionaryBcast, existingRowIDs.get)
    }

    val colIDs = filteredElements.map {
      case (_, colID) => colID
    }.distinct().collect()
    val colIDDictionary = new BiDictionary(keys = colIDs)
    val colIDDictionaryBcast = sc.broadcast(colIDDictionary)

    val ncol = colIDDictionary.size

    val indexedInteractions = filteredElements.map {
      case (rowID, colID) =>
        val rowIndex = rowIDDictionaryBcast.value.getOrElse(rowID, -1)
        val colIndex = colIDDictionaryBcast.value.getOrElse(colID, -1)
        rowIndex -> colIndex
    }.groupByKey().map {
      case (rowIndex, colIndexes) =>
        val row = new RandomAccessSparseVector(ncol)
        for (colIndex <- colIndexes) {
          row.setQuick(colIndex, 1.0)
        }
        rowIndex -> row
    }.asInstanceOf[DrmRdd[Int]].repartition(sc.defaultMinPartitions)

    val drmInteractions = drmWrap[Int](indexedInteractions)

    new IndexedDatasetSpark(
      drmInteractions.newRowCardinality(
        rowIDDictionary.size
      ),
      rowIDDictionary, colIDDictionary
    )
  }
}