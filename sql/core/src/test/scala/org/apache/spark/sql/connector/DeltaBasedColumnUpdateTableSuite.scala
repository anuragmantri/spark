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

  // --- Schema narrowing: verify LogicalWriteInfo.schema() is narrow ---

  test("column-update: rowSchema contains only the single assigned column") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    // Only the assigned column (id) should appear in the row schema -- not pk or dep
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

    // Both assigned columns (id, dep) should appear -- but NOT pk (unassigned)
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

  // --- Identity assignment filtering ---

  test("column-update: rowSchema excludes identity assignments in a mixed UPDATE") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    // id = id is identity -- should be excluded from rowSchema
    // dep = 'engineering' is a real assignment -- should be included
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

    // dep = dep is identity; id = -1 is a real assignment -- only id should appear
    sql(s"UPDATE $tableNameAsString SET dep = dep, id = -1 WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = false)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  // --- updatedColumns in RowLevelOperationInfo ---

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

    // dep = dep is identity; only id should appear in updatedColumns
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
    // DELETE never has updated columns -- verify that the default empty array is passed
    // through RowLevelOperationInfo even when a column-update connector handles the DELETE.
    // Use a partition-column condition so the InMemory table can process the filter.
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE dep = 'hr'")

    assert(table.lastUpdatedColumns.isEmpty,
      s"DELETE must pass empty updatedColumns but got: ${table.lastUpdatedColumns.mkString(", ")}")
  }

  // --- Data correctness ---

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

    // Only dep changes; id stays as-is even though id = id is in the SET list.
    sql(s"UPDATE $tableNameAsString SET id = id, dep = 'engineering' WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "engineering") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  // --- Scan narrowing: verify the connector only receives the columns it needs ---

  test("column-update: scan excludes the assigned column when SET to a literal") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    // id is the target of a literal assignment -- its current value is not needed.
    // pk is needed for the WHERE condition and as rowId.
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

    // salary appears on the RHS (salary * 2) so it must be scanned.
    // bonus is not referenced anywhere -- excluded.
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

    // dep is a literal assignment; id and salary are not referenced -- only pk needed.
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

    // dep appears in the WHERE clause -- must be scanned even though it is not assigned.
    // bonus is neither assigned nor in the condition -- excluded.
    // salary is set to a literal -- current value not needed.
    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    val scanSchema = table.lastScanSchema
    assert(scanSchema.fieldNames.contains("dep"),
      s"dep must be in scan (WHERE clause): $scanSchema")
    assert(!scanSchema.fieldNames.contains("bonus"), s"bonus should be excluded: $scanSchema")
    assert(!scanSchema.fieldNames.contains("salary"),
      s"salary should be excluded (literal assignment): $scanSchema")
  }

  // ---------------------------------------------------------------------------
  // Connector-driven scan narrowing via requiredDataAttributes()
  // ---------------------------------------------------------------------------

  // Creates a table backed by DeltaBasedColumnUpdateOperationWithReqAttrs, which overrides
  // requiredDataAttributes() to return the given comma-separated column names.
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
    // Connector declares it always needs "dep".
    // SQL assigns "id" (literal) with condition on "pk".
    // Connector-driven scan = {pk, dep} (dep from connector declaration; pk from condition).
    // id is NOT in scan: literal assignment + not declared by connector.
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
      s"id should be excluded (literal assignment, not declared): $scanSchema")
  }

  test("column-update: requiredDataAttributes - data correctness") {
    // Connector declares "dep,id" so it receives both the new id value and dep for routing.
    // The write schema is exactly requiredDataAttributes = {dep, id} (declared order).
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
    // "column-update" uses DeltaBasedColumnUpdateOperation whose requiredDataAttributes()
    // returns the default empty array.
    // With the optimizer-driven approach for MOR, the scan is narrowed by V2ScanRelationPushDown
    // which observes what columns the write plan actually references.
    // SET id = -1 (literal assignment): id is not referenced from the scan, so it is pruned.
    // dep is the partitioning column; since it is not declared in requiredDataAttributes()
    // and is not referenced by the WHERE condition (pk = 1), it may be pruned from the scan.
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    val scanSchema = table.lastScanSchema
    assert(!scanSchema.fieldNames.contains("id"),
      s"id must NOT be in scan (literal assignment, no scan reference): $scanSchema")
    assert(scanSchema.fieldNames.contains("pk"), s"pk must be in scan (condition): $scanSchema")
  }

  // ---------------------------------------------------------------------------
  // Connector uses RowLevelOperationInfo.updatedColumns() to derive its own
  // requiredDataAttributes() dynamically.
  // DeltaBasedColumnUpdateOperationFromInfo always adds "pk" (for row lookup) to
  // whatever Spark reports as updated columns.
  // ---------------------------------------------------------------------------

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
    // Connector receives updatedColumns=[salary], adds pk for row lookup.
    // requiredDataAttributes() = [pk, salary].
    //
    // salary = -1 is a LITERAL assignment: the write plan references Literal(-1) not the
    // scan's salary column.  Since salary is in assignedAttrs, it is not a connectorExtraAttr
    // pass-through either.  V2ScanRelationPushDown therefore does not see salary referenced
    // and prunes it from the scan.
    //
    // The scan contains: pk (connector pass-through), dep (partitioning + WHERE condition).
    // The scan excludes: salary (literal assignment), id and bonus (not declared, not in cond).
    createAndInitTableFromInfo("pk INT NOT NULL, salary INT, id INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "id": 10, "bonus": 5, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "id": 20, "bonus": 6, "dep": "software" }
        |{ "pk": 3, "salary": 300, "id": 30, "bonus": 7, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    val scanSchema = table.lastScanSchema
    assert(scanSchema.fieldNames.contains("pk"),
      s"pk must be in scan (connector pass-through via connectorExtraAttrs): $scanSchema")
    assert(scanSchema.fieldNames.contains("dep"),
      s"dep must be in scan (partitioning + WHERE): $scanSchema")
    assert(!scanSchema.fieldNames.contains("id"),
      s"id must be excluded (not declared, not assigned, not in condition): $scanSchema")
    assert(!scanSchema.fieldNames.contains("bonus"),
      s"bonus must be excluded (not declared, not assigned, not in condition): $scanSchema")
  }

  test("column-update from-info: write schema is updatedColumns + pk pass-through") {
    // requiredDataAttributes = [pk, salary] (pk always added; salary because it's assigned).
    // Write schema = requiredDataAttributes in declared order = {pk, salary}.
    createAndInitTableFromInfo("pk INT NOT NULL, salary INT, id INT, dep STRING",
      """{ "pk": 1, "salary": 100, "id": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "id": 20, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    val writeSchema = table.lastWriteInfo.schema()
    assert(writeSchema.fieldNames.contains("salary"),
      s"salary must be in write schema (assigned): $writeSchema")
    assert(writeSchema.fieldNames.contains("pk"),
      s"pk must be in write schema " +
        s"(connector pass-through via requiredDataAttributes): $writeSchema")
    assert(!writeSchema.fieldNames.contains("id"),
      s"id must not be in write schema: $writeSchema")
    assert(!writeSchema.fieldNames.contains("dep"),
      s"dep must not be in write schema (partitioning, not a data column to write): $writeSchema")
  }

  test("column-update from-info: pk already in updatedColumns is not duplicated") {
    // When the user updates pk itself, updatedColumns=[pk, salary].
    // Connector sees pk already present -> requiredDataAttributes=[pk, salary] (no dup).
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

    // salary updated for hr rows; id preserved (not in write schema, connector uses pk lookup)
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
  }

  // ---------------------------------------------------------------------------
  // CoW connector with supportsColumnUpdates() on RowLevelOperation.
  // PartitionBasedColumnUpdateOperation declares requiredDataAttributes() = [pk, dep] and
  // supportsColumnUpdates() = true.  Spark narrows the scan to connector-declared + assigned
  // columns; bonus is excluded.  The connector reconstructs full rows via pk lookup.
  // ---------------------------------------------------------------------------

  private def createAndInitTableCoW(schemaString: String, jsonData: String): Unit = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-cow", "true")
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

  test("column-update CoW: scan excludes columns not declared and not assigned") {
    // Connector declares [pk, dep].  SET salary = -1.
    // Narrow scan = pk (declared) + dep (declared + condition + partitioning)
    // + salary (assigned LHS).  bonus is neither declared nor assigned -> excluded.
    createAndInitTableCoW("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    val scanSchema = table.lastScanSchema
    assert(scanSchema.fieldNames.contains("pk"), s"pk must be in scan: $scanSchema")
    assert(scanSchema.fieldNames.contains("dep"), s"dep must be in scan: $scanSchema")
    assert(scanSchema.fieldNames.contains("salary"),
      s"salary must be in scan (assigned LHS): $scanSchema")
    assert(!scanSchema.fieldNames.contains("bonus"), s"bonus must be excluded: $scanSchema")
  }

  test("column-update CoW: write schema contains only declared + assigned columns") {
    createAndInitTableCoW("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    val writeSchema = table.lastWriteInfo.schema()
    assert(writeSchema.fieldNames.contains("pk"), s"pk must be in write schema: $writeSchema")
    assert(writeSchema.fieldNames.contains("dep"), s"dep must be in write schema: $writeSchema")
    assert(writeSchema.fieldNames.contains("salary"),
      s"salary must be in write schema: $writeSchema")
    assert(!writeSchema.fieldNames.contains("bonus"),
      s"bonus must not be in write schema: $writeSchema")
  }

  test("column-update CoW: data correctness -- bonus preserved, salary updated") {
    // bonus is not in the write schema; the connector must preserve it from the original row.
    createAndInitTableCoW("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
  }

  test("column-update CoW: narrow scan + subquery WHERE condition") {
    // Exercises buildReplaceDataWithUnionPlan + narrow scan + the flatMap change in
    // RowLevelOperationRuntimeGroupFiltering.buildTableToScanAttrMap.
    // The subquery forces the UNION path (updated rows + remaining rows).
    // bonus is not declared and not assigned, must be excluded from scan and write
    // but the subquery-based filter must still work correctly with the narrow scan.
    createAndInitTableCoW("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    import testImplicits._
    val subqueryDF = Seq("hr").toDF()
    subqueryDF.createOrReplaceTempView("target_deps")

    sql(
      s"""UPDATE $tableNameAsString
         |SET salary = -1
         |WHERE dep IN (SELECT * FROM target_deps)
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
  }

  // ---------------------------------------------------------------------------
  // Delta connector with representUpdateAsDeleteAndInsert=true AND supportsColumnUpdates=true.
  //
  // Point 7: The restriction that blocked column-level updates on the delete+reinsert path
  // has been removed.  The REINSERT leg of the Expand uses only assigned values (the narrow
  // write schema from effectiveRowAttrs), and the DELETE leg uses row ID only.
  // ---------------------------------------------------------------------------

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
    // representUpdateAsDeleteAndInsert=true + supportsColumnUpdates=true.
    // requiredDataAttributes() = [pk, id] (pk always declared; id because it's being updated).
    // The write schema = requiredDataAttributes() in declared order = {pk, id}.
    // dep is NOT in the write schema (not declared, not assigned).
    createAndInitTableSplit("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    // Write schema is exactly requiredDataAttributes = {pk, id} in declared order.
    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("id", IntegerType, nullable = false)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  test("column-update split: data correctness") {
    // representUpdateAsDeleteAndInsert=true + supportsColumnUpdates=true.
    // The connector receives narrow REINSERT rows and must reconstruct full rows.
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

