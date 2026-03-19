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
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Tests for UPDATE statements targeting connectors that implement SupportsColumnUpdate.
 *
 * When a connector implements SupportsColumnUpdate, Spark:
 *  1. Narrows the scan to only the columns needed to evaluate SET expressions and the
 *     WHERE condition (plus rowId and metadata columns), so unneeded columns are never
 *     read from the data files.
 *  2. Narrows the row projection (LogicalWriteInfo.schema()) sent to the connector via
 *     DeltaWriter.update to contain only the genuinely-changed (non-identity) columns.
 */
class DeltaBasedColumnUpdateTableSuite extends RowLevelOperationSuiteBase {

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update", "true")
    props
  }

  /**
   * Asserts that the scan projection pushed down to the connector by Spark's
   * V2ScanRelationPushDown contains exactly the expected column names.
   * The projection is recorded by DeltaBasedColumnUpdateOperation.newScanBuilder.
   */
  private def checkLastScanProjection(expectedColumnNames: Set[String]): Unit = {
    val actual = table.lastScanProjection
    assert(actual != null, "scan projection was not recorded; pruneColumns was never called")
    assert(
      actual.fieldNames.toSet == expectedColumnNames,
      s"scan projection mismatch:\n  expected: $expectedColumnNames\n  actual:   ${actual.fieldNames.toSet}")
  }

  // --- Write schema narrowing: verify LogicalWriteInfo.schema() is narrow ---

  test("column-update: rowSchema contains only the single assigned column") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    // Only the assigned column (id) should appear in the row schema — not pk or dep
    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = false)  // constant -1 is not nullable
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

    // Both assigned columns (id, dep) should appear — but NOT pk (unassigned)
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

    // Identity assignments: after alignUpdateAssignments, SET id=id, dep=dep produce
    // Assignment(idAttr, idAttr) — stripped by isIdentityAssignment and excluded
    sql(s"UPDATE $tableNameAsString SET id = id, dep = dep WHERE pk = 1")

    checkLastWriteInfo(
      expectedRowSchema = new StructType(),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))

    // data must be unchanged — identity assignments are no-ops
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "hr") :: Nil)
  }

  test("column-update: row filter condition is orthogonal to column narrowing") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    // The WHERE clause filters rows — column narrowing still applies to just dep
    sql(s"UPDATE $tableNameAsString SET dep = 'engineering' WHERE pk IN (1, 3)")

    // dep is the only changed column
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

    // Only salary should appear in rowSchema
    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("salary", IntegerType, nullable = true)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))
  }

  // --- Scan projection narrowing: verify only needed columns are read from storage ---

  test("column-update: scan excludes assignment-target column when SET uses a literal") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    // SET id = -1: -1 is a literal; id is only the assignment target, not on the RHS.
    // id does not need to be read from storage.
    // dep must appear because it is the table partition column (needed for outputPartitioning).
    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkLastScanProjection(Set("pk", "dep", "_partition"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("column-update: scan projects column referenced in SET expression, excludes others") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    // SET salary = salary * 2: salary is on the RHS and must be scanned.
    // bonus is not referenced in the SET or WHERE and is not the partition column -- excluded.
    sql(s"UPDATE $tableNameAsString SET salary = salary * 2")

    checkLastScanProjection(Set("pk", "salary", "dep", "_partition"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 200, 10, "hr") :: Row(2, 400, 20, "software") :: Row(3, 600, 30, "hr") :: Nil)
  }

  test("column-update: scan excludes LHS-only column for cross-column assignment") {
    createAndInitTable("pk INT NOT NULL, x INT, y INT, dep STRING",
      """{ "pk": 1, "x": 10, "y": 99, "dep": "hr" }
        |{ "pk": 2, "x": 20, "y": 55, "dep": "software" }
        |""".stripMargin)

    // SET x = y: y is on the RHS and must be scanned; x is only the assignment target.
    // x does not need to be read from storage -- its old value is irrelevant.
    sql(s"UPDATE $tableNameAsString SET x = y WHERE pk = 1")

    checkLastScanProjection(Set("pk", "y", "dep", "_partition"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 99, 99, "hr") :: Row(2, 20, 55, "software") :: Nil)
  }

  test("column-update: scan excludes column not referenced in SET or WHERE") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    // SET dep = 'engineering': dep is the assignment target and the partition column.
    // WHERE pk IN (1, 3): references pk (also the rowId).
    // id is not referenced anywhere and is not the partition column -- excluded.
    sql(s"UPDATE $tableNameAsString SET dep = 'engineering' WHERE pk IN (1, 3)")

    checkLastScanProjection(Set("pk", "dep", "_partition"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "engineering") :: Row(2, 2, "software") :: Row(3, 3, "engineering") :: Nil)
  }

  test("column-update: scan projects union of SET RHS and WHERE condition columns") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    // SET salary = salary + bonus: both salary and bonus referenced on RHS.
    // WHERE dep = 'hr': dep in condition AND is the partition column.
    sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'")

    checkLastScanProjection(Set("pk", "salary", "bonus", "dep", "_partition"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 110, 10, "hr") :: Row(2, 200, 20, "software") :: Row(3, 330, 30, "hr") :: Nil)
  }

  // --- Data correctness: verify values are correctly written and merged ---

  test("column-update: data correctness — single column update") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    // Only pk=1 is updated; pk=2 and pk=3 are unchanged
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("column-update: data correctness — update all rows") {
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

  test("column-update: cross-column assignment with same type is not treated as identity") {
    createAndInitTable("pk INT NOT NULL, x INT, y INT, dep STRING",
      """{ "pk": 1, "x": 10, "y": 99, "dep": "hr" }
        |{ "pk": 2, "x": 20, "y": 99, "dep": "hr" }
        |""".stripMargin)

    // x and y have the same type (INT); SET x = y is a real update, not an identity assignment.
    // isIdentityAssignment must rely on ExprId equality, not name+type, to distinguish them.
    sql(s"UPDATE $tableNameAsString SET x = y WHERE pk = 1")

    // x must appear in the write schema — it is genuinely being updated
    checkLastWriteInfo(
      expectedRowSchema = StructType(Seq(
        StructField("x", IntegerType, nullable = true)
      )),
      expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))

    // pk=1 should have x updated to 99; pk=2 unchanged
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 99, 99, "hr") :: Row(2, 20, 99, "hr") :: Nil)
  }
}

