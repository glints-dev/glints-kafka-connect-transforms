package com.glints.kafka.connect.transforms

import java.util.Collections

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TopicCamelCaseToSnakeCaseTest extends AnyFlatSpec with Matchers {
  val transform = new TopicCamelCaseToSnakeCase[SourceRecord]()

  it should "transform topic name to snake_case" in {
    val schema = SchemaBuilder
      .struct()
      .field("foo", Schema.STRING_SCHEMA)
      .build()

    val value = new Struct(schema)
    value.put("foo", "bar")

    val record = new SourceRecord(
      null,
      null,
      "TestHello.FooBar",
      0,
      null,
      null,
      schema,
      value,
    )

    val transformedRecord = transform.apply(record)
    transformedRecord.topic().shouldEqual("test_hello.foo_bar")
  }
}
