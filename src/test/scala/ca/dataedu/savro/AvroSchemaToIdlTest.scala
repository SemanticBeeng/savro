package ca.dataedu.savro

import org.apache.avro.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroSchemaToIdlTest extends AnyFlatSpec with Matchers {

  "toIdl" should "set the default values properly" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
        |    "name": "phone",
        |    "type": "long",
        |    "default": 0
        |  }, {
        |    "name": "lastName",
        |    "type": "string"
        |  }, {
        |    "name": "name",
        |    "type": ["null", "string"],
        |    "default": null
        |  }, {
        |    "name": "addresses",
        |    "type": {
        |      "type": "map",
        |      "values": {
        |        "type": "record",
        |        "name": "Address",
        |        "namespace": "ca.dataedu.avro",
        |        "fields": [{
        |          "name": "street",
        |          "type": "string"
        |        }, {
        |          "name": "city",
        |          "type": "string"
        |        }]
        |      }
        |    }
        |  }]
        |}""".stripMargin
    )
    new AvroSchemaToIdl(schema, "AvroSchemaTool").convert() shouldBe
    """@namespace("ca.dataedu.avro")
        |protocol AvroSchemaTool {
        |  record Person {
        |    map<Address> addresses;
        |    string lastName;
        |    union { null, string } name = null;
        |    long phone = 0;
        |  }
        |
        |  record Address {
        |    string city;
        |    string street;
        |  }
        |
        |}
        |""".stripMargin
  }

  "toIdl" should "0001_input.avsc" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "TestRecord",
        |  "namespace": "this.is.test.record.namespace",
        |  "doc": "Test record event docs",
        |  "fields": [
        |    {
        |      "name": "simpleStringField",
        |      "type": "string",
        |      "doc": "Simple string field doc"
        |    },
        |    {
        |      "name": "simpleNoDocStringField",
        |      "type": "string"
        |    },
        |    {
        |      "name": "simpleStringFieldWithDefault",
        |      "type": "string",
        |      "default": "default value"
        |    },
        |    {
        |      "name": "nullableStringFieldWithDefault",
        |      "type": ["null", "string"],
        |      "default": null
        |    }
        |  ]
        |}
        |""".stripMargin
    )
    new AvroSchemaToIdl(schema, "TestRecord").convert() shouldBe
    """@namespace("this.is.test.record.namespace")
      |protocol TestRecord {
      |    /** Test record event docs */
      |    record TestRecord {
      |        /** Simple string field doc */
      |        string simpleStringField;
      |        string simpleNoDocStringField;
      |        string simpleStringFieldWithDefault = "default value";
      |        union { null, string } nullableStringFieldWithDefault = null;
      |    }
      |}
      |""".stripMargin
  }

  "toIdl" should "0002_input.avsc" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type" : "record",
        |  "name" : "TestRecord",
        |  "namespace" : "this.is.test.record.namespace",
        |  "doc" : "This is TestRecord event docs",
        |  "fields" : [ {
        |    "name" : "someRecordA",
        |    "type" : [ "null", {
        |      "type" : "record",
        |      "name" : "NestedRecord",
        |      "doc" : "Nested record",
        |      "fields" : [ {
        |        "name" : "fieldWithUserDataType",
        |        "type" : "string",
        |        "doc" : "Nested record id",
        |        "userDataType" : "NestedRecordId"
        |      }, {
        |        "name" : "badges",
        |        "type" : {
        |          "type" : "array",
        |          "items" : {
        |            "type" : "record",
        |            "name" : "NestedArrayItem",
        |            "fields" : [ {
        |              "name" : "__hiddenData",
        |              "type" : [ "null", {
        |                "type" : "map",
        |                "values" : {
        |                  "type" : "record",
        |                  "name" : "NestedMapItem",
        |                  "fields" : [ {
        |                    "name" : "enumField",
        |                    "type" : {
        |                      "type" : "enum",
        |                      "name" : "SomeEnum",
        |                      "symbols" : [ "ABC", "XYZ", "THIRD" ]
        |                    }
        |                  } ]
        |                }
        |              } ],
        |              "doc" : "Hidden data",
        |              "default" : null
        |            } ]
        |          }
        |        }
        |      } ]
        |    } ],
        |    "doc" : "Some nested record",
        |    "default" : null
        |  }, {
        |    "name" : "someRecordB",
        |    "type" : "NestedRecord"
        |  } ]
        |}
        |""".stripMargin
    )
    new AvroSchemaToIdl(schema, "TestRecord").convert() shouldBe
    """@namespace("this.is.test.record.namespace")
      |protocol TestRecord {
      |    /** This is TestRecord event docs */
      |    record TestRecord {
      |        /** Some nested record */
      |        union { null, NestedRecord } someRecordA = null;
      |
      |        NestedRecord someRecordB;
      |    }
      |
      |    /** Nested record */
      |    record NestedRecord {
      |        /** Nested record id */
      |        string @userDataType("NestedRecordId") fieldWithUserDataType;
      |
      |        array<NestedArrayItem> badges;
      |    }
      |
      |    record NestedArrayItem {
      |        /** Hidden data */
      |        union { null, map<NestedMapItem> } __hiddenData = null;
      |    }
      |
      |    record NestedMapItem {
      |        SomeEnum enumField;
      |    }
      |
      |    enum SomeEnum {
      |        ABC,
      |        XYZ,
      |        THIRD
      |    }
      |}
      |""".stripMargin
  }

  "toIdl" should "0003_annotations.avsc" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type" : "record",
        |  "name" : "TestRecord",
        |  "namespace" : "this.is.test.record.namespace",
        |  "fields" : [ {
        |    "name" : "value",
        |    "type" : {
        |      "type" : "string",
        |      "java-class" : "java.math.BigDecimal"
        |    }
        |  }, {
        |    "name" : "typedKeys",
        |    "type" : {
        |      "type" : "map",
        |      "values" : "string",
        |      "java-key-class" : "java.math.BigDecimal"
        |    }
        |  }, {
        |    "name" : "typedValues",
        |    "type" : {
        |      "type" : "map",
        |      "values" : {
        |        "type" : "string",
        |        "java-class" : "java.math.BigDecimal"
        |      }
        |    }
        |  }, {
        |    "name" : "typedMixed",
        |    "type" : {
        |      "type" : "map",
        |      "values" : {
        |        "type" : "string",
        |        "java-class" : "java.math.BigDecimal"
        |      },
        |      "java-key-class" : "java.math.BigInt",
        |      "java-class" : "java.util.ArrayList"
        |    }
        |  }, {
        |      "name" : "typedMixedArray",
        |      "type" : {
        |        "type" : "array",
        |        "items" : {
        |          "type" : "string",
        |          "java-class" : "java.math.BigDecimal"
        |        },
        |        "java-class" : "java.util.HashMap"
        |      }
        |  } ]
        |}
        |""".stripMargin
    )
    new AvroSchemaToIdl(schema, "TestRecord").convert() shouldBe
    """@namespace("this.is.test.record.namespace")
      |protocol TestRecord {
      |    record TestRecord {
      |        @java-class("java.math.BigDecimal") string value;
      |
      |        @java-key-class("java.math.BigDecimal") map<string> typedKeys;
      |
      |        map<@java-class("java.math.BigDecimal") string> typedValues;
      |
      |        @java-key-class("java.math.BigInt") @java-class("java.util.ArrayList") map<@java-class("java.math.BigDecimal") string> typedMixed;
      |
      |        @java-class("java.util.HashMap") array<@java-class("java.math.BigDecimal") string> typedMixedArray;
      |    }
      |}
      |""".stripMargin
  }

}
