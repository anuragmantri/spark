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
 * Tests for UPDATE statements targeting connectors that return true from
 * [[org.apache.spark.sql.connector.write.SupportsDelta#supportsColumnUpdates]].
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
    // The connector receives a narrow row containing only dep and must leave id untouched.
    sql(s"UPDATE $tableNameAsString SET id = id, dep = 'engineering' WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "engineering") :: Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }
}
