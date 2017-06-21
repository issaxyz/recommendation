package org.template

import java.util

import grizzled.slf4j.Logger
import org.apache.predictionio.data.storage._
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.{ ImmutableSettings, Settings }
import org.joda.time.DateTime
import org.json4s.jackson.JsonMethods._
import org.elasticsearch.spark._
import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.search.SearchHits
import org.json4s.JValue
import org.template.conversions.{ ItemID, ItemProps }

import scala.collection.immutable
import scala.collection.parallel.mutable

object EsClient {
  @transient lazy val logger: Logger = Logger[this.type]

  private lazy val client = if (Storage.getConfig("ELASTICSEARCH").nonEmpty)
    new elasticsearch.StorageClient(Storage.getConfig("ELASTICSEARCH").get).client
  else
    throw new IllegalStateException("No Elasticsearch client configuration detected, check your pio-env.sh for" +
      "proper configuration settings")

  def deleteIndex(indexName: String, refresh: Boolean = false): Boolean = {
    //val debug = client.connectedNodes()
    if (client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists) {
      val delete = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
      if (!delete.isAcknowledged) {
        logger.info(s"Index $indexName wasn't deleted, but may have quietly failed.")
      } else {
        if (refresh) refreshIndex(indexName)
      }
      true
    } else {
      logger.warn(s"Elasticsearch index: $indexName wasn't deleted because it didn't exist. This may be an error.")
      false
    }
  }

  def createIndex(
    indexName: String,
    indexType: String,
    fieldNames: List[String],
    typeMappings: Map[String, String] = Map.empty,
    refresh: Boolean = false): Boolean = {
    if (!client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists) {
      var mappings = """
        |{
        |  "properties": {
        """.stripMargin.replace("\n", "")

      def mappingsField(`type`: String) = {
        s"""
        |    : {
        |      "type": "${`type`}",
        |      "index": "not_analyzed",
        |      "norms" : {
        |        "enabled" : false
        |      }
        |    },
        """.stripMargin.replace("\n", "")
      }

      val mappingsTail = """
        |    "id": {
        |      "type": "string",
        |      "index": "not_analyzed",
        |      "norms" : {
        |        "enabled" : false
        |      }
        |    }
        |  }
        |}
      """.stripMargin.replace("\n", "")

      fieldNames.foreach { fieldName =>
        if (typeMappings.contains(fieldName))
          mappings += (fieldName + mappingsField(typeMappings(fieldName)))
        else // unspecified fields are treated as not_analyzed strings
          mappings += (fieldName + mappingsField("string"))
      }
      mappings += mappingsTail // any other string is not_analyzed

      val cir = new CreateIndexRequest(indexName).mapping(indexType, mappings)
      val create = client.admin().indices().create(cir).actionGet()
      if (!create.isAcknowledged) {
        logger.info(s"Index $indexName wasn't created, but may have quietly failed.")
      } else {
        if (refresh) refreshIndex(indexName)
      }
      true
    } else {
      logger.warn(s"Elasticsearch index: $indexName wasn't created because it already exists. This may be an error.")
      false
    }
  }

  def refreshIndex(indexName: String): Unit = {
    client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet()
  }

  def hotSwap(
    alias: String,
    typeName: String,
    indexRDD: RDD[Map[String, Any]],
    fieldNames: List[String],
    typeMappings: Map[String, String] = Map.empty): Unit = {
    val aliasMetadata = client.admin().indices().prepareGetAliases(alias).get().getAliases
    val newIndex = alias + "_" + DateTime.now().getMillis.toString

    logger.debug(s"Create new index: $newIndex, $typeName, $fieldNames, $typeMappings")
    createIndex(newIndex, typeName, fieldNames, typeMappings)

    val newIndexURI = "/" + newIndex + "/" + typeName
    indexRDD.saveToEs(newIndexURI, Map("es.mapping.id" -> "id"))
    //refreshIndex(newIndex)

    if (!aliasMetadata.isEmpty
      && aliasMetadata.get(alias) != null
      && aliasMetadata.get(alias).get(0) != null) { // was alias so remove the old one
      //append the DateTime to the alias to create an index name
      val oldIndex = aliasMetadata.get(alias).get(0).getIndexRouting
      client.admin().indices().prepareAliases()
        .removeAlias(oldIndex, alias)
        .addAlias(newIndex, alias)
        .execute().actionGet()
      deleteIndex(oldIndex)
    } else {
      val indices = util.Arrays.asList(client.admin().indices().prepareGetIndex().get().indices()).get(0)
      if (indices.contains(alias)) {
        //refreshIndex(alias)
        deleteIndex(alias) // index named like the new alias so delete it
      }
      client.admin().indices().prepareAliases()
        .addAlias(newIndex, alias)
        .execute().actionGet()
    }
    val indices = util.Arrays.asList(client.admin().indices().prepareGetIndex().get().indices()).get(0)
    indices.map { index =>
      if (index.contains(alias) && index != newIndex) deleteIndex(index)
    }

  }

  def search(query: String, indexName: String): Option[SearchHits] = {
    val sr = client.prepareSearch(indexName).setSource(query).get()

    if (!sr.isTimedOut) {
      Some(sr.getHits)
    } else {
      None
    }
  }

  def getSource(indexName: String, typeName: String, doc: String): util.Map[String, AnyRef] = {
    client.prepareGet(indexName, typeName, doc)
      .execute()
      .actionGet().getSource
  }

  def getIndexName(alias: String): Option[String] = {

    val allIndicesMap = client.admin().indices().getAliases(new GetAliasesRequest(alias)).actionGet().getAliases

    if (allIndicesMap.size() == 1) { // must be a 1-1 mapping of alias <-> index
      var indexName: String = ""
      val itr = allIndicesMap.keysIt()
      while (itr.hasNext)
        indexName = itr.next()
      Some(indexName) // the one index the alias points to
    } else {
      logger.warn("There is no 1-1 mapping of index to alias so deleting the old indexes that are referenced by the " +
        "alias. This may have been caused by a crashed or stopped `pio train` operation so try running it again.")
      if (!allIndicesMap.isEmpty) {
        val i = allIndicesMap.keys().toArray.asInstanceOf[Array[String]]
        for (indexName <- i) {
          deleteIndex(indexName, refresh = true)
        }
      }
      None // if more than one abort, need to clean up bad aliases
    }
  }

  def getRDD(
    alias: String,
    typeName: String)(implicit sc: SparkContext): RDD[(ItemID, ItemProps)] = {
    getIndexName(alias)
      .map(index => sc.esJsonRDD(alias + "/" + typeName) map { case (itemId, json) => itemId -> DataMap(json).fields })
      .getOrElse(sc.emptyRDD)
  }
}
