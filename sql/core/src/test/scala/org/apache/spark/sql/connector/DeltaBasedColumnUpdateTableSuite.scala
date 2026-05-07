/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, TableInfo}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{identity, reference}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Tests for UPDATE statements targeting connectors that return true from
 * [[org.apache.spark.sql.connector.write.RowLevelOperation#supportsColumnUpdates]].
 *
 * When a connector supports column updates, Spark narrows the row projection
 * (LogicalWriteInfo.schema()) to contain only the assigned/changed columns rather than
 * the full table row.
 */
class DeltaBasedColumnUpdateTableSuite extends RowLevelOperationSuiteBase {

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update", "true")
    props
  }

  test("column-update: rowSchema contains only the single assigned column") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = false)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  test("column-update: rowSchema contains multiple assigned columns") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1, dep = 'engineering' WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("dep", StringType, nullable = false)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  test("column-update: rowSchema is empty for a full identity update") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = dep WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowSchema = new StructType(),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  test("column-update: row filter condition is orthogonal to column narrowing") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET dep = 'engineering' WHERE pk IN (1, 3)")

    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("dep", StringType, nullable = false)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  test("column-update: update all rows (no WHERE clause)") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = salary * 2")

    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("salary", IntegerType, nullable = true)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  test("column-update: rowSchema excludes identity assignments in a mixed UPDATE") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = 'engineering' WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("dep", StringType, nullable = false)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  test("column-update: cross-column assignment is not treated as identity") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET dep = dep, id = -1 WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = false)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  test("column-update: nested struct field update narrows to the root struct column") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames == Set("s"),
      s"expected [s] in updatedColumns (root struct) but got: $updatedNames")

    val writeSchema = table.lastWriteInfo.schema()
    assert(writeSchema.fieldNames.contains("s"),
      s"s must be in write schema: $writeSchema")
    assert(!writeSchema.fieldNames.contains("dep"),
      s"dep must not be in write schema: $writeSchema")
  }

  test("column-update: updatedColumns contains non-identity assigned columns") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1, dep = 'eng' WHERE pk = 1")

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames == Set("id", "dep"),
      s"expected [id, dep] in updatedColumns but got: $updatedNames")
  }

  test("column-update: updatedColumns excludes identity assignments") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1, dep = dep WHERE pk = 1")

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames == Set("id"),
      s"expected only [id] in updatedColumns but got: $updatedNames")
  }

  test("column-update: updatedColumns is empty for a full identity update") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = dep WHERE pk = 1")

    assert(table.lastUpdatedColumns.isEmpty,
      s"expected empty updatedColumns but got: ${table.lastUpdatedColumns.mkString(", ")}")
  }

  test("column-update: updatedColumns is empty for DELETE (Javadoc contract)") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE dep = 'hr'")

    assert(table.lastUpdatedColumns.isEmpty,
      s"DELETE must pass empty updatedColumns but got: ${table.lastUpdatedColumns.mkString(", ")}")
  }

  test("column-update: data correctness -- single column update") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("column-update: data correctness -- update all rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = salary * 2")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 200, "hr") :: Row(2, 400, "software") :: Row(3, 600, "hr") :: Nil)
  }

  test("column-update: data correctness -- mixed identity and real assignments") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = id, dep = 'engineering' WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "engineering") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("column-update: scan excludes the assigned column when SET to a literal") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    val scanSchema = table.lastScanSchema
    assert(!scanSchema.fieldNames.contains("id"), s"id should be excluded from scan: $scanSchema")
    assert(scanSchema.fieldNames.contains("pk"), s"pk must be in scan: $scanSchema")
  }

  test("column-update: scan includes the assigned column when its current value is the RHS") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = salary * 2")

    val scanSchema = table.lastScanSchema
    assert(scanSchema.fieldNames.contains("salary"), s"salary must be in scan: $scanSchema")
    assert(!scanSchema.fieldNames.contains("bonus"), s"bonus should be excluded: $scanSchema")
  }

  test("column-update: scan excludes non-referenced columns for literal assignment") {
    createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
      """{ "pk": 1, "id": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET dep = 'engineering' WHERE pk = 1")

    val scanSchema = table.lastScanSchema
    assert(scanSchema.fieldNames.contains("pk"), s"pk must be in scan: $scanSchema")
    assert(!scanSchema.fieldNames.contains("id"), s"id should be excluded: $scanSchema")
    assert(!scanSchema.fieldNames.contains("salary"), s"salary should be excluded: $scanSchema")
  }

  test("column-update: scan includes condition columns even when not assigned") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |""".stripMargin)

    // dep is in WHERE but not assigned -- must still be scanned
    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    val scanSchema = table.lastScanSchema
    assert(scanSchema.fieldNames.contains("dep"),
      s"dep must be in scan (WHERE clause): $scanSchema")
    assert(!scanSchema.fieldNames.contains("bonus"), s"bonus should be excluded: $scanSchema")
    assert(!scanSchema.fieldNames.contains("salary"),
      s"salary should be excluded (literal assignment): $scanSchema")
  }

  private def createAndInitTableWithReqAttrs(
      reqAttrs: String,
      schemaString: String,
      jsonData: String): Unit = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-req-attrs", reqAttrs)
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    val transforms = Array[Transform](identity(reference(Seq("dep"))))
    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withPartitions(transforms)
      .withProperties(props)
      .build()
    catalog.createTable(ident, tableInfo)
    append(schemaString, jsonData)
  }

  test("column-update: requiredDataAttributes forces connector-declared column into scan") {
    createAndInitTableWithReqAttrs("dep", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    val scanSchema = table.lastScanSchema
    assert(scanSchema.fieldNames.contains("dep"),
      s"dep must be in scan (connector required): $scanSchema")
    assert(scanSchema.fieldNames.contains("pk"), s"pk must be in scan: $scanSchema")
    assert(!scanSchema.fieldNames.contains("id"),
      s"id should be excluded (literal, not declared): $scanSchema")
  }

  test("column-update: requiredDataAttributes - data correctness") {
    createAndInitTableWithReqAttrs("dep,id", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("column-update: empty requiredDataAttributes falls back to heuristic") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    val scanSchema = table.lastScanSchema
    assert(!scanSchema.fieldNames.contains("id"),
      s"id must NOT be in scan (literal assignment): $scanSchema")
    assert(scanSchema.fieldNames.contains("pk"), s"pk must be in scan (condition): $scanSchema")
  }

  test("column-update: requiredDataAttributes throws AnalysisException for invalid column") {
    createAndInitTableWithReqAttrs("nonexistent_col", "pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    val ex = intercept[org.apache.spark.sql.AnalysisException] {
      sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
    }
    assert(ex.getMessage.contains("nonexistent_col"),
      s"Expected error about unresolvable column but got: ${ex.getMessage}")
  }

  private def createAndInitTableFromInfo(schemaString: String, jsonData: String): Unit = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-from-info", "true")
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    val transforms = Array[Transform](identity(reference(Seq("dep"))))
    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withPartitions(transforms)
      .withProperties(props)
      .build()
    catalog.createTable(ident, tableInfo)
    append(schemaString, jsonData)
  }

  test("column-update from-info: connector adds pk to updatedColumns for requiredDataAttributes") {
    createAndInitTableFromInfo("pk INT NOT NULL, salary INT, id INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "id": 10, "bonus": 5, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "id": 20, "bonus": 6, "dep": "software" }
        |{ "pk": 3, "salary": 300, "id": 30, "bonus": 7, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    val scanSchema = table.lastScanSchema
    assert(scanSchema.fieldNames.contains("pk"), s"pk must be in scan: $scanSchema")
    assert(scanSchema.fieldNames.contains("dep"), s"dep must be in scan (WHERE): $scanSchema")
    assert(!scanSchema.fieldNames.contains("id"), s"id must be excluded: $scanSchema")
    assert(!scanSchema.fieldNames.contains("bonus"), s"bonus must be excluded: $scanSchema")
  }

  test("column-update from-info: write schema is updatedColumns + pk pass-through") {
    createAndInitTableFromInfo("pk INT NOT NULL, salary INT, id INT, dep STRING",
      """{ "pk": 1, "salary": 100, "id": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "id": 20, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    val writeSchema = table.lastWriteInfo.schema()
    assert(writeSchema.fieldNames.contains("salary"), s"salary must be in write schema: $writeSchema")
    assert(writeSchema.fieldNames.contains("pk"), s"pk must be in write schema: $writeSchema")
    assert(!writeSchema.fieldNames.contains("id"), s"id must not be in write schema: $writeSchema")
    assert(!writeSchema.fieldNames.contains("dep"),
      s"dep must not be in write schema: $writeSchema")
  }

  test("column-update from-info: pk already in updatedColumns is not duplicated") {
    createAndInitTableFromInfo("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET pk = pk + 10, salary = -1 WHERE dep = 'hr'")

    val writeSchema = table.lastWriteInfo.schema()
    val pkCount = writeSchema.fieldNames.count(_ == "pk")
    assert(pkCount == 1, s"pk must appear exactly once in write schema: $writeSchema")
  }

  test("column-update from-info: data correctness") {
    createAndInitTableFromInfo("pk INT NOT NULL, salary INT, id INT, dep STRING",
      """{ "pk": 1, "salary": 100, "id": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "id": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "id": 30, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
  }

  private def createAndInitTableSplit(schemaString: String, jsonData: String): Unit = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-split", "true")
    val columns = CatalogV2Util.structTypeToV2Columns(StructType.fromDDL(schemaString))
    val transforms = Array[Transform](identity(reference(Seq("dep"))))
    val tableInfo = new TableInfo.Builder()
      .withColumns(columns)
      .withPartitions(transforms)
      .withProperties(props)
      .build()
    catalog.createTable(ident, tableInfo)
    append(schemaString, jsonData)
  }

  test("column-update split: write schema is narrow (assigned + pk pass-through)") {
    createAndInitTableSplit("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("id", IntegerType, nullable = false)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  test("column-update split: data correctness") {
    createAndInitTableSplit("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, -1, "hr") :: Nil)
  }
}
