
import breeze.linalg.norm
import breeze.optimize.linear.PowerMethod.BDV
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object ItemSimilarityAlgo {
  private val spark = SparkUtils.getSparkSession("similarityCalc")

  def main(args: Array[String]): Unit = {
    val t2vRddByCat = getText2VecRdd()
    println("t2vRddByCat calcd successfully!")
    val itemSimByCat = calcSimilarity(t2vRddByCat)
    println("itemSimByCat calcd successfully!")
    if (saveItemSimilarity(itemSimByCat)) {
      print("Item similarity has been saved!")
    }
  }

  def calcSimilarity(
                      t2vRddByCat: List[(String, RDD[(String, String, DenseVector)])],
                      neighborNum: Option[Int]=None): List[(String, RDD[(String, String, String)])] = {
    t2vRddByCat.map {
      case (cat4cat, t2vRdd) =>
        val bT2vRdd = spark.sparkContext.broadcast(t2vRdd.collect())
        val itemSim = t2vRdd.map {
          case (pId1, catPath, t2v1) =>
            val t2v1BDV = new BDV(t2v1.values)
            val t2vs = bT2vRdd.value.filter(_._1 != pId1)
            val neighbors = t2vs.map {
              case (pId2, _, t2v2) =>
                val t2v2BDV = new BDV(t2v2.values)
                val cosSim = t2v2BDV.dot(t2v1BDV) / (norm(t2v2BDV) * norm(t2v1BDV))
                (pId2, cosSim)
            }.sortWith(_._2 > _._2).take(neighborNum.getOrElse(ItemSimParams.neighborNum))
            val simPayloads = neighbors.map {
              case (pId2, simScore) => pId2 + "|" + simScore.formatted("%.5f")
            }
            (pId1, catPath, simPayloads.mkString(" "))
        }
        cat4cat -> itemSim
    }
  }

  def getText2VecRdd(
                      w2vDim: Option[Int]=None,
                      minCount: Option[Int]=None): List[(String, RDD[(String, String, DenseVector)])] = {
    val itemDescData: DataFrame = DataSource.readingItemDesc()

    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("text_vec")
      .setVectorSize(w2vDim.getOrElse(ItemSimParams.w2vDim))
      .setMinCount(minCount.getOrElse(ItemSimParams.minCount))

    val model = word2Vec.fit(itemDescData)

    import spark.implicits._
    val t2vRdd = model.transform(itemDescData).select(
      "product_id", "cat_path", "cat4cat", "text_vec"
    ).map(
      row => {
        val catPath = row.get(1).toString
        val cat4cat = row.get(2).toString
        val productID = row.get(0).toString
        val textVec = row.get(3).asInstanceOf[DenseVector]
        cat4cat -> (productID, catPath, textVec)
      }).rdd

    val allCat4cat = t2vRdd.map {
      case (cat4cat, _) => cat4cat
    }.distinct().collect().toList

    allCat4cat.map {
      cat4cat =>
        val singleT2vRdd = t2vRdd.filter {
          record => cat4cat.equals(record._1)
        }.map {
          record => (record._2._1, record._2._2, record._2._3)
        }
        (cat4cat, singleT2vRdd)
    }
  }

  def saveItemSimilarity(itemSim: List[(String, RDD[(String, String, String)])]): Boolean = {
    val itemSimHead = itemSim.head
    var similarityDF: DataFrame = spark.createDataFrame(
      itemSimHead._2)
      .toDF("product_id", "cat_path" , "text_corr_payload")

    itemSim.tail.foreach {
      case (category, singleRdd) =>
        val tmpDF = spark.createDataFrame(singleRdd)
          .toDF("product_id", "cat_path" , "text_corr_payload")
        similarityDF = similarityDF.union(tmpDF)
    }

    similarityDF.write.mode("Overwrite").format("hive"
    ).insertInto("ai.nc_item_similarity")
    true
  }
}


