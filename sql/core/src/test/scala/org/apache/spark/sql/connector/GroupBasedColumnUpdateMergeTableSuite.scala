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
import org.apache.spark.sql.types.StructType

/**
 * Tests for MERGE INTO column-level updates against connectors that take the ReplaceData
 * (CoW) path with `supportsColumnUpdates() = true`.
 */
class GroupBasedColumnUpdateMergeTableSuite extends RowLevelOperationSuiteBase {

  private def createMergeTargetTable(schemaString: String, jsonData: String): Unit = {
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

  private def createSourceView(viewName: String, rows: Seq[Row], schemaString: String): Unit = {
    val schema = StructType.fromDDL(schemaString)
    val rdd = spark.sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, schema)
    df.createOrReplaceTempView(viewName)
  }

  test("merge column-update: updatedColumns contains assigned columns from matched UPDATE") {
    createMergeTargetTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |""".stripMargin)

    createSourceView("source", Seq(Row(1, 999)), "pk INT, salary INT")

    sql(
      s"""MERGE INTO $tableNameAsString t
         |USING source s
         |ON t.pk = s.pk
         |WHEN MATCHED THEN UPDATE SET t.salary = s.salary
         |""".stripMargin)

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames == Set("salary"),
      s"expected [salary] in updatedColumns but got: $updatedNames")
  }

  test("merge column-update: updatedColumns excludes identity assignments") {
    createMergeTargetTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |""".stripMargin)

    createSourceView("source", Seq(Row(1, 999)), "pk INT, salary INT")

    // bonus = bonus is identity and must NOT appear in updatedColumns
    sql(
      s"""MERGE INTO $tableNameAsString t
         |USING source s
         |ON t.pk = s.pk
         |WHEN MATCHED THEN UPDATE SET t.salary = s.salary, t.bonus = t.bonus
         |""".stripMargin)

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames == Set("salary"),
      s"expected only [salary] in updatedColumns but got: $updatedNames")
  }

  test("merge column-update: updateSchema is narrow when there are no INSERT actions") {
    createMergeTargetTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |""".stripMargin)

    createSourceView("source", Seq(Row(1, 999)), "pk INT, salary INT")

    sql(
      s"""MERGE INTO $tableNameAsString t
         |USING source s
         |ON t.pk = s.pk
         |WHEN MATCHED THEN UPDATE SET t.salary = s.salary
         |""".stripMargin)

    val info = table.lastWriteInfo
    assert(info.updateSchema().isPresent, "updateSchema must be set for MERGE column-update")
    val updateSchema = info.updateSchema().get()
    val updateNames = updateSchema.fieldNames.toSet
    assert(updateNames.contains("salary"), s"updateSchema must contain salary: $updateSchema")
    assert(updateNames.contains("pk"),
      s"updateSchema must contain pk (connector-required): $updateSchema")
    assert(!updateNames.contains("bonus"), s"bonus must not be in updateSchema: $updateSchema")
  }

  test("merge column-update: updateSchema is narrow even when INSERT is present") {
    createMergeTargetTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |""".stripMargin)

    createSourceView("source",
      Seq(Row(1, 999), Row(99, 500)),
      "pk INT, salary INT")

    sql(
      s"""MERGE INTO $tableNameAsString t
         |USING source s
         |ON t.pk = s.pk
         |WHEN MATCHED THEN UPDATE SET t.salary = s.salary
         |WHEN NOT MATCHED THEN INSERT (pk, salary, bonus, dep) VALUES (s.pk, s.salary, 0, 'new')
         |""".stripMargin)

    val info = table.lastWriteInfo
    assert(info.updateSchema().isPresent,
      "updateSchema must be set when connector supports column updates")

    val updateNames = info.updateSchema().get().fieldNames.toSet
    assert(updateNames.contains("salary"), "updateSchema must contain assigned salary")
    assert(!updateNames.contains("bonus"), "updateSchema must not contain unassigned bonus")

    val schemaNames = info.schema().fieldNames.toSet
    assert(schemaNames == Set("pk", "salary", "bonus", "dep"),
      s"schema (INSERT path) must be full table: ${info.schema()}")
  }

  test("merge column-update: data correctness for matched UPDATE only") {
    createMergeTargetTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "eng" }
        |""".stripMargin)

    createSourceView("source",
      Seq(Row(1, 111), Row(2, 222)),
      "pk INT, salary INT")

    sql(
      s"""MERGE INTO $tableNameAsString t
         |USING source s
         |ON t.pk = s.pk
         |WHEN MATCHED THEN UPDATE SET t.salary = s.salary
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 111, 10, "hr") ::
      Row(2, 222, 20, "hr") ::
      Row(3, 300, 30, "eng") :: Nil)
  }

  test("merge column-update: data correctness for matched UPDATE + INSERT") {
    createMergeTargetTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |""".stripMargin)

    createSourceView("source",
      Seq(Row(1, 111), Row(99, 500)),
      "pk INT, salary INT")

    sql(
      s"""MERGE INTO $tableNameAsString t
         |USING source s
         |ON t.pk = s.pk
         |WHEN MATCHED THEN UPDATE SET t.salary = s.salary
         |WHEN NOT MATCHED THEN INSERT (pk, salary, bonus, dep) VALUES (s.pk, s.salary, 0, 'hr')
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 111, 10, "hr") ::
      Row(2, 200, 20, "hr") ::
      Row(99, 500, 0, "hr") :: Nil)
  }

  test("merge column-update: data correctness for matched DELETE + matched UPDATE") {
    createMergeTargetTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    createSourceView("source",
      Seq(Row(1, 111, "update"), Row(2, 0, "delete")),
      "pk INT, salary INT, op STRING")

    sql(
      s"""MERGE INTO $tableNameAsString t
         |USING source s
         |ON t.pk = s.pk
         |WHEN MATCHED AND s.op = 'delete' THEN DELETE
         |WHEN MATCHED AND s.op = 'update' THEN UPDATE SET t.salary = s.salary
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 111, 10, "hr") ::
      Row(3, 300, 30, "hr") :: Nil)
  }

  test("merge column-update: full identity update keeps original values") {
    createMergeTargetTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |""".stripMargin)

    createSourceView("source", Seq(Row(1)), "pk INT")

    sql(
      s"""MERGE INTO $tableNameAsString t
         |USING source s
         |ON t.pk = s.pk
         |WHEN MATCHED THEN UPDATE SET t.salary = t.salary, t.bonus = t.bonus
         |""".stripMargin)

    val updatedNames = table.lastUpdatedColumns.map(_.describe()).toSet
    assert(updatedNames.isEmpty,
      s"all-identity MERGE UPDATE must produce empty updatedColumns: $updatedNames")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 100, 10, "hr") :: Nil)
  }
}
