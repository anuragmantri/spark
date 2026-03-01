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
 * When a connector implements SupportsColumnUpdate, Spark narrows the row projection
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
    // Assignment(idAttr, idAttr) where value.semanticEquals(key) — filtered out
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
