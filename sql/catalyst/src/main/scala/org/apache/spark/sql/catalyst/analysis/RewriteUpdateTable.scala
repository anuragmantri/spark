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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Cast, EqualNullSafe, Expression, If, Literal, MetadataAttribute, Not, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Expand, Filter, LogicalPlan, Project, ReplaceData, Union, UpdateTable, WriteDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.{RowLevelOperationTable, SupportsDelta}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2Table}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A rule that rewrites UPDATE operations using plans that operate on individual or groups of rows.
 *
 * This rule assumes the commands have been fully resolved and all assignments have been aligned.
 */
object RewriteUpdateTable extends RewriteRowLevelCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UpdateTable(aliasedTable, assignments, cond)
        if u.resolved && u.rewritable && u.aligned =>

      EliminateSubqueryAliases(aliasedTable) match {
        case r @ ExtractV2Table(tbl: SupportsRowLevelOperations) =>
          val table = buildOperationTable(tbl, UPDATE, CaseInsensitiveStringMap.empty())
          val updateCond = cond.getOrElse(TrueLiteral)
          table.operation match {
            case _: SupportsDelta =>
              buildWriteDeltaPlan(r, table, assignments, updateCond)
            case _ if SubqueryExpression.hasSubquery(updateCond) =>
              buildReplaceDataWithUnionPlan(r, table, assignments, updateCond)
            case _ =>
              buildReplaceDataPlan(r, table, assignments, updateCond)
          }

        case _ =>
          u
      }
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  // if the condition does NOT contain a subquery
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {

    // resolve all required metadata attrs that may be used for grouping data on write
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    // construct a read relation and include all required metadata columns
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)

    // build a plan with updated and copied over records
    val updatedAndRemainingRowsPlan = buildReplaceDataUpdateProjection(
      readRelation, assignments, cond)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = operationTable)
    val query = addOperationColumn(WRITE_WITH_METADATA_OPERATION, updatedAndRemainingRowsPlan)
    val projections = buildReplaceDataProjections(query, relation.output, metadataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    ReplaceData(writeRelation, cond, query, relation, projections, groupFilterCond)
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  // if the condition contains a subquery
  private def buildReplaceDataWithUnionPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {

    // resolve all required metadata attrs that may be used for grouping data on write
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    // construct a read relation and include all required metadata columns
    // the same read relation will be used to read records that must be updated and copied over
    // the analyzer will take care of duplicated attr IDs
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)

    // build a plan for updated records that match the condition
    val matchedRowsPlan = Filter(cond, readRelation)
    val updatedRowsPlan = buildReplaceDataUpdateProjection(matchedRowsPlan, assignments)

    // build a plan that contains unmatched rows in matched groups that must be copied over
    val remainingRowFilter = Not(EqualNullSafe(cond, Literal.TrueLiteral))
    val remainingRowsPlan = Filter(remainingRowFilter, readRelation)

    // the new state is a union of updated and copied over records
    val updatedAndRemainingRowsPlan = Union(updatedRowsPlan, remainingRowsPlan)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = operationTable)
    val query = addOperationColumn(WRITE_WITH_METADATA_OPERATION, updatedAndRemainingRowsPlan)
    val projections = buildReplaceDataProjections(query, relation.output, metadataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    ReplaceData(writeRelation, cond, query, relation, projections, groupFilterCond)
  }

  // this method assumes the assignments have been already aligned before
  private def buildReplaceDataUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression = TrueLiteral): LogicalPlan = {

    // the plan output may include metadata columns at the end
    // that's why the number of assignments may not match the number of plan output columns
    val assignedValues = assignments.map(_.value)
    val updatedValues = plan.output.zipWithIndex.map { case (attr, index) =>
      if (index < assignments.size) {
        val assignedExpr = assignedValues(index)
        val updatedValue = If(cond, assignedExpr, attr)
        Alias(updatedValue, attr.name)()
      } else {
        assert(MetadataAttribute.isValid(attr.metadata))
        if (MetadataAttribute.isPreservedOnUpdate(attr)) {
          attr
        } else {
          val updatedValue = If(cond, Literal(null, attr.dataType), attr)
          Alias(updatedValue, attr.name)(explicitMetadata = Some(attr.metadata))
        }
      }
    }

    Project(updatedValues, plan)
  }

  // build a rewrite plan for sources that support row deltas
  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): WriteDelta = {

    val operation = operationTable.operation.asInstanceOf[SupportsDelta]
    val supportsColumnUpdate =
      operation.supportsColumnUpdates() && !operation.representUpdateAsDeleteAndInsert

    // resolve all needed attrs (e.g. row ID and any required metadata attrs)
    val rowIdAttrs = resolveRowIdAttrs(relation, operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)

    // construct a read relation: narrow to required columns when column updates are supported,
    // otherwise include all table columns
    val readRelation = if (supportsColumnUpdate) {
      buildNarrowReadRelation(
        relation, operationTable, assignments, cond, rowIdAttrs, metadataAttrs,
        operationTable.table.partitioning())
    } else {
      buildRelationWithAttrs(relation, operationTable, metadataAttrs, rowIdAttrs)
    }

    // build a plan for updated records that match the condition
    val matchedRowsPlan = Filter(cond, readRelation)
    val rowDeltaPlan = if (operation.representUpdateAsDeleteAndInsert) {
      buildDeletesAndInserts(matchedRowsPlan, assignments, rowIdAttrs)
    } else if (supportsColumnUpdate) {
      buildColumnUpdateProjection(matchedRowsPlan, assignments, rowIdAttrs, metadataAttrs)
    } else {
      buildWriteDeltaUpdateProjection(matchedRowsPlan, assignments, rowIdAttrs)
    }

    // when column update is supported, narrow rowAttrs to only the genuinely-assigned columns
    // so LogicalWriteInfo.schema() carries a partial schema
    val effectiveRowAttrs = if (supportsColumnUpdate) {
      computeAssignedAttrs(assignments)
    } else {
      relation.output
    }

    // build a plan to write the row delta to the table
    val writeRelation = relation.copy(table = operationTable)
    val projections = buildWriteDeltaProjections(
      rowDeltaPlan, effectiveRowAttrs, rowIdAttrs, metadataAttrs)
    WriteDelta(writeRelation, cond, rowDeltaPlan, relation, projections)
  }

  // Builds a narrow read relation that only scans the columns required for a column update:
  // - attribute references from the RHS of non-identity assignments (to compute new values)
  // - attributes referenced in the WHERE condition
  // - row ID attributes (to identify the row for DeltaWriter#update)
  // - required metadata attributes (for partition grouping/ordering)
  // Uses AttributeSet for all set operations to avoid direct exprId comparisons.
  private def buildNarrowReadRelation(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression,
      rowIdAttrs: Seq[AttributeReference],
      metadataAttrs: Seq[AttributeReference],
      partitioning: Array[org.apache.spark.sql.connector.expressions.Transform]
      ): DataSourceV2Relation = {

    val assignmentRhsAttrs = AttributeSet(assignments.collect {
      case Assignment(key: Attribute, value) if !isIdentityAssignment(key, value) => value
    })
    val condAttrs = AttributeSet(Seq(cond))

    // partition columns must be included so V2ScanPartitioningAndOrdering can resolve
    // the partitioning expressions from the scan relation output
    val partitioningAttrs = AttributeSet(
      partitioning.flatMap { transform =>
        transform.references().flatMap { ref =>
          relation.output.find(attr => conf.resolver(attr.name, ref.describe()))
        }
      }
    )

    val required = assignmentRhsAttrs ++ condAttrs ++ AttributeSet(rowIdAttrs) ++ partitioningAttrs

    // filter relation.output to only required data columns, preserving original order
    val narrowOutput = relation.output.filter(required.contains)
    relation.copy(
      table = operationTable,
      output = dedupAttrs(narrowOutput ++ rowIdAttrs ++ metadataAttrs))
  }

  // Builds the row delta projection for the column update path. Unlike
  // buildWriteDeltaUpdateProjection (which is position-based and requires a full scan),
  // this method works with a narrow scan by looking up columns by name.
  private def buildColumnUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute]): LogicalPlan = {

    // only emit values for non-identity assignments (the narrow write schema)
    val assignedValues = assignments.collect {
      case Assignment(key: Attribute, value) if !isIdentityAssignment(key, value) =>
        Alias(value, key.name)()
    }

    // pass through or null out metadata columns present in the narrow scan
    val metadataAttrSet = AttributeSet(metadataAttrs)
    val metadataValues = plan.output.filter(metadataAttrSet.contains).map { attr =>
      if (MetadataAttribute.isPreservedOnUpdate(attr)) {
        attr
      } else {
        Alias(Literal(null, attr.dataType), attr.name)(explicitMetadata = Some(attr.metadata))
      }
    }

    // pass through row ID columns from the narrow scan
    val rowIdAttrSet = AttributeSet(rowIdAttrs)
    val rowIdValues = plan.output.filter(rowIdAttrSet.contains)

    val originalRowIdValues = buildOriginalRowIdValues(rowIdAttrs, assignments)
    val operationType = Alias(Literal(UPDATE_OPERATION), OPERATION_COLUMN)()

    Project(
      Seq(operationType) ++ assignedValues ++ metadataValues ++ rowIdValues ++ originalRowIdValues,
      plan)
  }

  // Returns only the table attributes that are genuinely updated (changed) in this UPDATE.
  // After alignUpdateAssignments, the value is always wrapped in Alias(...)(explicitMetadata)
  // by TableOutputResolver.applyColumnMetadata, and may also be wrapped in Cast for type
  // coercion. Strip these wrappers before comparing by exprId via AttributeSet.
  private def computeAssignedAttrs(assignments: Seq[Assignment]): Seq[Attribute] = {
    assignments.collect {
      case Assignment(key: Attribute, value) if !isIdentityAssignment(key, value) => key
    }
  }

  private def isIdentityAssignment(key: Attribute, value: Expression): Boolean = {
    stripAliasesAndCasts(value) match {
      case attr: Attribute => AttributeSet(Seq(key)).contains(attr)
      case _ => false
    }
  }

  // Recursively strips Alias and Cast wrappers introduced during assignment alignment.
  private def stripAliasesAndCasts(expr: Expression): Expression = expr match {
    case Alias(child, _) => stripAliasesAndCasts(child)
    case Cast(child, _, _, _) => stripAliasesAndCasts(child)
    case other => other
  }

  // this method assumes the assignments have been already aligned before
  private def buildWriteDeltaUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute]): LogicalPlan = {

    // the plan output may include immutable metadata columns at the end
    // that's why the number of assignments may not match the number of plan output columns
    val assignedValues = assignments.map(_.value)
    val updatedValues = plan.output.zipWithIndex.map { case (attr, index) =>
      if (index < assignments.size) {
        val assignedExpr = assignedValues(index)
        Alias(assignedExpr, attr.name)()
      } else {
        assert(MetadataAttribute.isValid(attr.metadata))
        if (MetadataAttribute.isPreservedOnUpdate(attr)) {
          attr
        } else {
          Alias(Literal(null, attr.dataType), attr.name)(explicitMetadata = Some(attr.metadata))
        }
      }
    }

    // original row ID values must be preserved and passed back to the table to encode updates
    // if there are any assignments to row ID attributes, add extra columns for the original values
    val originalRowIdValues = buildOriginalRowIdValues(rowIdAttrs, assignments)

    val operationType = Alias(Literal(UPDATE_OPERATION), OPERATION_COLUMN)()

    Project(Seq(operationType) ++ updatedValues ++ originalRowIdValues, plan)
  }

  private def buildDeletesAndInserts(
      matchedRowsPlan: LogicalPlan,
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute]): Expand = {

    val (metadataAttrs, rowAttrs) = matchedRowsPlan.output.partition { attr =>
      MetadataAttribute.isValid(attr.metadata)
    }
    val deleteOutput = deltaDeleteOutput(rowAttrs, rowIdAttrs, metadataAttrs)
    val insertOutput = deltaReinsertOutput(assignments, metadataAttrs)
    val outputs = Seq(deleteOutput, insertOutput)
    val operationTypeAttr = AttributeReference(OPERATION_COLUMN, IntegerType, nullable = false)()
    val attrs = operationTypeAttr +: matchedRowsPlan.output
    val expandOutput = generateExpandOutput(attrs, outputs)
    Expand(outputs, expandOutput, matchedRowsPlan)
  }
}
