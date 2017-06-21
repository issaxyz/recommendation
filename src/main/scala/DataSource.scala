package org.template

import _root_.org.apache.predictionio.controller.{ EmptyActualResult, EmptyEvaluationInfo, PDataSource, Params }
import _root_.org.apache.predictionio.data.storage.PropertyMap
import _root_.org.apache.predictionio.data.store.PEventStore
import grizzled.slf4j.Logger
import org.apache.predictionio.core.{ EventWindow, SelfCleaningDataSource }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.template.conversions.{ ActionID, ItemID }
import org.template.conversions._

case class DataSourceParams(
  appName: String,
  eventNames: List[String], // IMPORTANT: eventNames must be exactly the same as URAlgorithmParams eventNames
  eventWindow: Option[EventWindow]) extends Params

class DataSource(val dsp: DataSourceParams)
    extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, EmptyActualResult]
    with SelfCleaningDataSource {

  @transient override lazy implicit val logger: Logger = Logger[this.type]

  override def appName: String = dsp.appName
  override def eventWindow: Option[EventWindow] = dsp.eventWindow

  drawInfo("Init DataSource", Seq(
    ("══════════════════════════════", "════════════════════════════"),
    ("App name", appName),
    ("Event window", eventWindow),
    ("Event names", dsp.eventNames)))

  override def readTraining(sc: SparkContext): TrainingData = {

    val eventNames = dsp.eventNames

    val eventsRDD = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(eventNames),
      targetEntityType = Some(Some("item")))(sc).repartition(sc.defaultParallelism)

    val eventRDDs: List[(ActionID, RDD[(UserID, ItemID)])] = eventNames.map { eventName =>
      val singleEventRDD = eventsRDD.filter { event =>
        require(eventNames.contains(event.event), s"Unexpected event $event is read.") // is this really needed?
        require(event.entityId.nonEmpty && event.targetEntityId.get.nonEmpty, "Empty user or item ID")
        eventName.equals(event.event)
      }.map { event =>
        (event.entityId, event.targetEntityId.get)
      }

      (eventName, singleEventRDD)
    } filterNot { case (_, singleEventRDD) => singleEventRDD.isEmpty() }

    logger.info(s"Received events ${eventRDDs.map(_._1)}")
    logger.info(s"Number of events ${eventRDDs.map(_._1.length)}")

    val fieldsRDD: RDD[(ItemID, PropertyMap)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item")(sc).repartition(sc.defaultParallelism)
    //    logger.debug(s"FieldsRDD\n${fieldsRDD.take(25).mkString("\n")}")

    TrainingData(eventRDDs, fieldsRDD)
  }
}

case class TrainingData(
    actions: Seq[(ActionID, RDD[(UserID, ItemID)])],
    fieldsRDD: RDD[(ItemID, PropertyMap)]) extends Serializable {

  override def toString: String = {
    val a = actions.map { t =>
      s"${t._1} actions: [count:${t._2.count()}] + sample:${t._2.take(2).toList} "
    }.toString()
    val f = s"Item metadata: [count:${fieldsRDD.count}] + sample:${fieldsRDD.take(2).toList} "
    a + f
  }

}
