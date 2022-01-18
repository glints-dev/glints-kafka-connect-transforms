package com.glints.kafka.connect.transforms

import java.util.Collections
import java.{util => ju}

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Schema

import com.glints.kafka.connect.transforms.CamelCaseToSnakeCase
import org.apache.kafka.connect.data.Struct

class CamelCaseToSnakeCaseTest extends AnyFlatSpec with Matchers {
  val transformKey = new CamelCaseToSnakeCase.Key[SourceRecord]()
  val transformValue = new CamelCaseToSnakeCase.Value[SourceRecord]()

  it should "transform key property names to snake_case without schema" in {
    val record = new SourceRecord(
      null,
      null,
      "test",
      0,
      null,
      Collections.singletonMap("camelCase", 42L),
      null,
      null
    )

    val transformedRecord = transformKey.apply(record)
    val transformedKey =
      transformedRecord.key().asInstanceOf[ju.Map[String, Object]]

    transformedKey.containsKey("camel_case") shouldBe true
    transformedKey.get("camel_case") shouldBe 42L
  }

  it should "transform value property names to snake_case without schema" in {
    val record = new SourceRecord(
      null,
      null,
      "test",
      0,
      null,
      null,
      null,
      Collections.singletonMap("camelCase", 42L)
    )

    val transformedRecord = transformValue.apply(record)
    val transformedValue =
      transformedRecord.value().asInstanceOf[ju.Map[String, Object]]

    transformedValue.containsKey("camel_case") shouldBe true
    transformedValue.get("camel_case") shouldBe 42L
  }

  it should "transform value property names to snake_case with schema" in {
    val schema = SchemaBuilder
      .struct()
      .field("camelCase", Schema.INT64_SCHEMA)
      .field("PascalCase", Schema.INT64_SCHEMA)
      .build()

    val value = new Struct(schema)
    value.put("camelCase", 42L)
    value.put("PascalCase", 42L)

    val record = new SourceRecord(
      null,
      null,
      "test",
      0,
      null,
      null,
      schema,
      value
    )

    val transformedRecord = transformValue.apply(record)
    val transformedValue =
      transformedRecord.value().asInstanceOf[Struct]

    transformedValue.getInt64("camel_case") shouldBe 42L
    transformedValue.getInt64("pascal_case") shouldBe 42L
  }
}
