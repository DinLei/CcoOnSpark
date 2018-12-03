
import org.apache.mahout.math.cf.{DownsamplableCrossOccurrenceDataset, SimilarityAnalysis}
import org.apache.mahout.math.indexeddataset.IndexedDataset
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}
import helpers._

object CCOAlgorithm {
  implicit val sc: SparkContext = SparkUtils.getSparkContext("ccoAlgoSC")

  def main(args: Array[String]): Unit = {
    var endDate: String = ""
    var diff: Int = 0

    if (args.length == 1) {
      endDate = DateUtil.getNowDate()
      diff = Integer.parseInt(args(0))
    } else if (args.length == 2) {
      endDate = args(0)
      diff = Integer.parseInt(args(1))
    }

    var startDate: String = DateUtil.getDiffDate(Some(endDate), Some(diff))

    val dataSource: DataSource = new DataSource(
      Some(1), startDate, endDate)
    val trainingData: TrainingData = dataSource.readingTraining()

    val preparator: Preparator = new Preparator()
    val preparedData: PreparedData = preparator.prepare(
      trainingData
    )
    val indicatorsOp = ModelParams.indicatorParamsList

    val cooccurrenceIDSs: List[IndexedDataset] = calcCorrMatrix(
      preparedData, indicatorsOp
    )
    val cooccurrenceCorrelators = cooccurrenceIDSs.zip(
      preparedData.events.map(_._1)).map(_.swap)
    println("Cooccurance matrix has been calculated ojbk!")

    val ifSuccessfullySaved = saveCorrMatrix(
      SparkUtils.getSparkSession("saveMatrix"),
      cooccurrenceCorrelators)
    println("ifSuccessfullySaved: " + ifSuccessfullySaved)
  }

  def calcCorrMatrix(
                      preparedData: PreparedData,
                      indicatorsOp: Option[List[IndicatorParams]] = None
                    ): List[IndexedDataset] = {
    if (indicatorsOp.isEmpty) {
      SimilarityAnalysis.cooccurrencesIDSs(
        preparedData.events.map(_._2).toArray,
        maxInterestingItemsPerThing = DefaultModelParams.MaxCorrelatorsPerEventType,
        maxNumInteractions = DefaultModelParams.MaxEventsPerEventType
      ).map(_.asInstanceOf[IndexedDatasetSpark])
    } else {
      val indicators = indicatorsOp.get
      val iDs = preparedData.events.map(_._2)
      val datasets: List[DownsamplableCrossOccurrenceDataset] =
        iDs.zipWithIndex.map {
          case (iD, i) =>
            new DownsamplableCrossOccurrenceDataset(
              iD,
              indicators(i).MaxEventsPerEventType.get,
              indicators(i).MaxCorrelatorsPerEventType.get
            )
        }.toList

      SimilarityAnalysis.crossOccurrenceDownsampled(
        datasets, 1024).map(_.asInstanceOf[IndexedDatasetSpark])
    }
  }

  def saveCorrMatrix(
                    spark: SparkSession,
                    cooccurrenceCorrelators: List[(String, IndexedDataset)]
          ): Boolean = {
    val correlatorRDDs: Seq[RDD[itemEventCorr]] = cooccurrenceCorrelators.map {
      case (eventName, dataSet) =>
        dataSet.asInstanceOf[IndexedDatasetSpark].toPayloadCorrRDD(eventName)
    }

    var lastOutput: RDD[itemEventCorr] = correlatorRDDs.head
    val size: Int = correlatorRDDs.size
    println("We finally got " + size + "correlatorRDD!")
    if (size > 1) {
      for (i <- 1 until size) {
        lastOutput = lastOutput.union(correlatorRDDs(i))
      }
    }
    spark.createDataFrame(lastOutput).toDF(
      "product_id", "corr_payload", "event"
    ).write.mode("Overwrite").format("hive"
    ).insertInto("ai.nc_item_event_corr")
    true
  }
}

