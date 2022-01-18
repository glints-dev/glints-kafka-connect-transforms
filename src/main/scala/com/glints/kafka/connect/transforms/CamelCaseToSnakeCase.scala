package com.glints.kafka.connect.transforms

import java.{util => ju}
import scala.jdk.CollectionConverters._

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.util.SimpleConfig

import org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.connect.data.Struct

object CamelCaseToSnakeCase {
  class Key[R <: ConnectRecord[R]] extends CamelCaseToSnakeCase[R] {
    override protected def operatingValue(record: R): Object = {
      return record.key()
    }

    override protected def operatingSchema(record: R): Schema = {
      return record.keySchema()
    }

    override protected def newRecord(
        record: R,
        updatedSchema: Schema,
        updatedValue: Object
    ): R = {
      return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        updatedSchema,
        updatedValue,
        record.valueSchema(),
        record.value(),
        record.timestamp()
      )
    }
  }

  class Value[R <: ConnectRecord[R]] extends CamelCaseToSnakeCase[R] {
    override protected def operatingValue(record: R): Object = {
      return record.value()
    }

    override protected def operatingSchema(record: R): Schema = {
      return record.valueSchema()
    }

    override protected def newRecord(
        record: R,
        updatedSchema: Schema,
        updatedValue: Object
    ): R = {
      return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        updatedSchema,
        updatedValue,
        record.timestamp()
      )
    }
  }
}

abstract class CamelCaseToSnakeCase[R <: ConnectRecord[R]]
    extends Transformation[R] {

  val CONFIG_DEF = new ConfigDef
  val PURPOSE = "camel case to snake case transformation";
  val PATTERN = "([A-Z])".r

  var schemaUpdateCache: Cache[Schema, Schema] = new SynchronizedCache(
    new LRUCache(16)
  )

  override def configure(props: ju.Map[String, _ <: Object]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, props)
  }

  override def apply(record: R): R = {
    if (operatingValue(record) == null) {
      return record
    } else if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private def applySchemaless(record: R): R = {
    val value = requireMap(operatingValue(record), PURPOSE)
    val updatedValue = new ju.HashMap[String, Object](value.size())

    for (field <- value.entrySet().asScala) {
      val fieldName = field.getKey()
      updatedValue.put(renamed(fieldName), field.getValue())
    }

    return newRecord(record, null, updatedValue)
  }

  private def applyWithSchema(record: R): R = {
    val value = requireStruct(operatingValue(record), PURPOSE)

    val schema = Option(schemaUpdateCache.get(value.schema()))
    val updatedSchema = schema match {
      case Some(value) => value
      case None => {
        val updatedSchema = makeUpdatedSchema(value.schema())
        schemaUpdateCache.put(value.schema(), updatedSchema)
        updatedSchema
      }
    }

    val updatedValue = new Struct(updatedSchema)

    for (field <- value.schema().fields().asScala) {
      val fieldValue = value.get(field.name())
      updatedValue.put(renamed(field.name()), fieldValue)
    }

    return newRecord(record, updatedSchema, updatedValue)
  }

  private def makeUpdatedSchema(schema: Schema): Schema = {
    val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct())

    for (field <- schema.fields().asScala) {
      builder.field(renamed(field.name()), field.schema())
    }

    return builder.build()
  }

  private def renamed(fieldName: String): String = {
    return (fieldName.charAt(0) + PATTERN
      .replaceAllIn(fieldName.slice(1, fieldName.length()), "_$1"))
      .toLowerCase()
  }

  override def config(): ConfigDef = {
    return CONFIG_DEF
  }

  override def close(): Unit = {}

  protected def operatingSchema(record: R): Schema
  protected def operatingValue(record: R): Object
  protected def newRecord(
      record: R,
      updatedSchema: Schema,
      updatedValue: Object
  ): R
}
