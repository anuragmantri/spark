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

class GroupBasedColumnUpdateTableSuite extends RowLevelOperationSuiteBase {

  private def createAndInitTableReplaceData(schemaString: String, jsonData: String): Unit = {
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

  test("column-update ReplaceData: scan excludes columns not declared and not assigned") {
    createAndInitTableReplaceData("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    val scanSchema = table.lastScanSchema
    assert(scanSchema.fieldNames.contains("pk"), s"pk must be in scan: $scanSchema")
    assert(scanSchema.fieldNames.contains("dep"), s"dep must be in scan: $scanSchema")
    assert(scanSchema.fieldNames.contains("salary"),
      s"salary must be in scan (assigned): $scanSchema")
    assert(!scanSchema.fieldNames.contains("bonus"), s"bonus must be excluded: $scanSchema")
  }

  test("column-update ReplaceData: write schema contains only declared + assigned columns") {
    createAndInitTableReplaceData("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
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

  test("column-update ReplaceData: data correctness -- bonus preserved, salary updated") {
    createAndInitTableReplaceData("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
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

  test("column-update ReplaceData: narrow scan + subquery WHERE condition") {
    createAndInitTableReplaceData("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
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
}
