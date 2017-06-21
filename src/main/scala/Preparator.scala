
package org.template

import org.apache.predictionio.controller.PPreparator
import org.apache.mahout.math.indexeddataset.{ BiDictionary, IndexedDataset }
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.template.conversions._

class Preparator
    extends PPreparator[TrainingData, PreparedData] {
  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    var userDictionary: Option[BiDictionary] = None

    val indexedDatasets = trainingData.actions.map {
      case (eventName, eventIDS) =>

        val ids = IndexedDatasetSpark(eventIDS, userDictionary)(sc)
        userDictionary = Some(ids.rowIDs)
        (eventName, ids)
    }

    // now make sure all matrices have identical row space since this corresponds to all users
    // with the primary event since other users do not contribute to the math
    val rowAdjustedIds = userDictionary map { userDict =>
      indexedDatasets.map {
        case (eventName, eventIDS) =>
          (eventName, eventIDS.create(eventIDS.matrix, userDictionary.get, eventIDS.columnIDs)
            .newRowCardinality(userDict.size)) // force row cardinality and sharing userDict
      }
    } getOrElse Seq.empty

    val fieldsRDD: RDD[(ItemID, ItemProps)] = trainingData.fieldsRDD.map {
      case (itemId, propMap) => itemId -> propMap.fields
    }
    PreparedData(rowAdjustedIds, fieldsRDD)
  }

}

case class PreparedData(
  actions: Seq[(ActionID, IndexedDataset)],
  fieldsRDD: RDD[(ItemID, ItemProps)]) extends Serializable
