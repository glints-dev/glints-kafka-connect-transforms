package com.glints.kafka.connect.transforms

import java.util.Objects
import scala.jdk.CollectionConverters._

import io.debezium.config.Configuration
import io.debezium.data.Envelope
import io.debezium.transforms.SmtManager
import io.debezium.util.SchemaNameAdjuster
import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

class TopicCamelCaseToSnakeCase[R <: ConnectRecord[R]]
    extends Transformation[R] {
  val CONFIG_DEF = new ConfigDef
  val PURPOSE = "camel case to snake case transformation";
  val PATTERN = "([A-Z])".r

  val schemaNameAdjuster = SchemaNameAdjuster.create()
  val smtManager = new SmtManager[R](Configuration.empty())

  var keySchemaUpdateCache: Cache[Schema, Schema] = new SynchronizedCache(
    new LRUCache(16)
  )

  var envelopeSchemaUpdateCache: Cache[Schema, Schema] = new SynchronizedCache(
    new LRUCache(16)
  )

  override def configure(props: java.util.Map[String, _ <: Object]): Unit = {}

  override def apply(record: R): R = {
    val oldTopic = record.topic()
    val newTopic = determineNewTopic(oldTopic)

    val key = Option(record.key())

    // Key could be null in the case of a table without a primary key
    val newKeySchema = key match {
      case None => null
      case Some(value) => {
        val oldKey = requireStruct(record.key(), "Updating schema");
        updateKeySchema(oldKey.schema(), newTopic);
      }
    }

    val newKey = key match {
      case None => null
      case Some(value) => {
        val oldKey = requireStruct(record.key(), "Updating schema");
        updateKey(newKeySchema, oldKey, oldTopic);
      }
    }

    // In case of tombstones or non-CDC events (heartbeats, schema change events),
    // leave the value as-is
    if (record.value() == null || !smtManager.isValidEnvelope(record)) {
      // Value will be null in the case of a delete event tombstone
      return record.newRecord(
        newTopic,
        record.kafkaPartition(),
        newKeySchema,
        newKey,
        record.valueSchema(),
        record.value(),
        record.timestamp()
      );
    }

    val oldEnvelope = requireStruct(record.value(), "Updating schema");
    val newEnvelopeSchema =
      updateEnvelopeSchema(oldEnvelope.schema(), newTopic);
    val newEnvelope = updateEnvelope(newEnvelopeSchema, oldEnvelope);

    return record.newRecord(
      newTopic,
      record.kafkaPartition(),
      newKeySchema,
      newKey,
      newEnvelopeSchema,
      newEnvelope,
      record.timestamp()
    )
  }

  private def determineNewTopic(topic: String): String = {
    return topic
      .split("\\.")
      .map(part =>
        (part.charAt(0) + PATTERN
          .replaceAllIn(part.slice(1, part.length()), "_$1"))
          .toLowerCase()
      )
      .mkString(".")
  }

  private def updateKey(
      newKeySchema: Schema,
      oldKey: Struct,
      oldTopic: String
  ): Struct = {
    val newKey = new Struct(newKeySchema);

    for (field <- oldKey.schema().fields().asScala) {
      newKey.put(field.name(), oldKey.get(field));
    }

    return newKey;
  }

  private def updateKeySchema(
      oldKeySchema: Schema,
      newTopicName: String
  ): Schema = {
    val builder = copySchemaExcludingName(oldKeySchema, SchemaBuilder.struct());
    builder.name(schemaNameAdjuster.adjust(newTopicName + ".Key"));

    val newKeySchema =
      Option(keySchemaUpdateCache.get(oldKeySchema)) match {
        case Some(value) => {
          return value
        }
        case None => builder.build()
      }

    keySchemaUpdateCache.put(oldKeySchema, newKeySchema);
    return newKeySchema;
  }

  private def updateEnvelope(
      newEnvelopeSchema: Schema,
      oldEnvelope: Struct
  ): Struct = {
    val newEnvelope = new Struct(newEnvelopeSchema);
    val newValueSchema =
      newEnvelopeSchema.field(Envelope.FieldName.BEFORE).schema();
    for (field <- oldEnvelope.schema().fields().asScala) {
      val fieldName = field.name();
      var fieldValue = oldEnvelope.get(field);
      if (
        (Objects.equals(fieldName, Envelope.FieldName.BEFORE) || Objects.equals(
          fieldName,
          Envelope.FieldName.AFTER
        ))
        && fieldValue != null
      ) {
        fieldValue = updateValue(
          newValueSchema,
          requireStruct(fieldValue, "Updating schema")
        );
      }
      newEnvelope.put(fieldName, fieldValue);
    }

    return newEnvelope;
  }

  private def updateEnvelopeSchema(
      oldEnvelopeSchema: Schema,
      newTopicName: String
  ): Schema = {
    var newEnvelopeSchema = envelopeSchemaUpdateCache.get(oldEnvelopeSchema);
    if (newEnvelopeSchema != null) {
      return newEnvelopeSchema;
    }

    val oldValueSchema =
      oldEnvelopeSchema.field(Envelope.FieldName.BEFORE).schema();
    val valueBuilder =
      copySchemaExcludingName(oldValueSchema, SchemaBuilder.struct());
    valueBuilder.name(schemaNameAdjuster.adjust(newTopicName + ".Value"));
    val newValueSchema = valueBuilder.build();

    val envelopeBuilder =
      copySchemaExcludingName(oldEnvelopeSchema, SchemaBuilder.struct(), false);
    for (field <- oldEnvelopeSchema.fields().asScala) {
      val fieldName = field.name();
      var fieldSchema = field.schema();
      if (
        Objects.equals(fieldName, Envelope.FieldName.BEFORE) || Objects.equals(
          fieldName,
          Envelope.FieldName.AFTER
        )
      ) {
        fieldSchema = newValueSchema;
      }
      envelopeBuilder.field(fieldName, fieldSchema);
    }
    envelopeBuilder.name(
      schemaNameAdjuster.adjust(Envelope.schemaName(newTopicName))
    );

    newEnvelopeSchema = envelopeBuilder.build();
    envelopeSchemaUpdateCache.put(oldEnvelopeSchema, newEnvelopeSchema);
    return newEnvelopeSchema;
  }

  private def updateValue(newValueSchema: Schema, oldValue: Struct): Struct = {
    val newValue = new Struct(newValueSchema);
    for (field <- oldValue.schema().fields().asScala) {
      newValue.put(field.name(), oldValue.get(field));
    }
    return newValue;
  }

  private def copySchemaExcludingName(
      source: Schema,
      builder: SchemaBuilder
  ): SchemaBuilder = {
    return copySchemaExcludingName(source, builder, true);
  }

  private def copySchemaExcludingName(
      source: Schema,
      builder: SchemaBuilder,
      copyFields: Boolean
  ): SchemaBuilder = {
    builder.version(source.version());
    builder.doc(source.doc());

    val params = Option(source.parameters()) match {
      case Some(value) => builder.parameters(value)
      case None        => {}
    };

    if (source.isOptional()) {
      builder.optional();
    } else {
      builder.required();
    }

    if (copyFields) {
      for (field <- source.fields().asScala) {
        builder.field(field.name(), field.schema());
      }
    }

    return builder;
  }

  override def config(): ConfigDef = {
    return CONFIG_DEF
  }

  override def close(): Unit = {}
}
