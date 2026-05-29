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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, EqualNullSafe, Expression, If, Literal, MetadataAttribute, Not, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Expand, Filter, LogicalPlan, Project, ReplaceData, Union, UpdateTable, WriteDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.expressions.FieldReference
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
          val updatedCols = assignments.collect {
            case Assignment(key: AttributeReference, value) if !isIdentityAssignment(key, value) =>
              FieldReference(key.name)
          }
          val table = buildOperationTable(tbl, UPDATE, CaseInsensitiveStringMap.empty(),
            updatedCols)
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

    val (readRelation, rowAttrs, metadataAttrs) =
      buildReplaceDataReadRelation(relation, operationTable, assignments, cond)

    val updatedAndRemainingRowsPlan = buildReplaceDataUpdateProjection(
      readRelation, assignments, cond)

    val writeRelation = relation.copy(table = operationTable)
    val projections = buildReplaceDataProjections(updatedAndRemainingRowsPlan, rowAttrs,
      metadataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    ReplaceData(writeRelation, cond, updatedAndRemainingRowsPlan, relation, projections,
      groupFilterCond)
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  // if the condition contains a subquery
  private def buildReplaceDataWithUnionPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {

    val (readRelation, rowAttrs, metadataAttrs) =
      buildReplaceDataReadRelation(relation, operationTable, assignments, cond)

    // build a plan for updated records that match the condition
    val matchedRowsPlan = Filter(cond, readRelation)
    val updatedRowsPlan = buildReplaceDataUpdateProjection(matchedRowsPlan, assignments)

    // build a plan that contains unmatched rows in matched groups that must be copied over
    val remainingRowFilter = Not(EqualNullSafe(cond, Literal.TrueLiteral))
    val remainingRowsPlan = addOperationColumn(COPY_OPERATION,
      Filter(remainingRowFilter, readRelation))

    val updatedAndRemainingRowsPlan = Union(updatedRowsPlan, remainingRowsPlan)

    val writeRelation = relation.copy(table = operationTable)
    val projections = buildReplaceDataProjections(updatedAndRemainingRowsPlan, rowAttrs,
      metadataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    ReplaceData(writeRelation, cond, updatedAndRemainingRowsPlan, relation, projections,
      groupFilterCond)
  }

  /**
   * When the connector supports column updates and declares required data attributes,
   * the read relation is narrowed at analysis time so that GroupBasedRowLevelOperationScanPlanning
   * uses only the needed columns for the scan. Otherwise, the full relation output is used.
   */
  private def buildReplaceDataReadRelation(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): (DataSourceV2Relation, Seq[Attribute], Seq[AttributeReference]) = {

    val operation = operationTable.operation
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)
    val connectorDataAttrs = resolveRequiredDataAttrs(relation, operation)
    val isNarrow = operation.supportsColumnUpdates() && connectorDataAttrs.nonEmpty

    val readRelation = if (isNarrow) {
      val allRequired = (connectorDataAttrs ++ computeAssignedAttrs(assignments)).distinct
      buildRelationWithAttrs(relation, operationTable, metadataAttrs, dataAttrs = allRequired,
        cond = cond)
    } else {
      buildRelationWithAttrs(relation, operationTable, metadataAttrs)
    }

    val rowAttrs: Seq[Attribute] = if (isNarrow) connectorDataAttrs else relation.output

    (readRelation, rowAttrs, metadataAttrs)
  }

  /**
   * Builds the update projection for ReplaceData plans. Assumes assignments are already aligned.
   *
   * plan.output may be narrowed by buildReplaceDataReadRelation, so only columns present in the
   * plan are projected.
   */
  private def buildReplaceDataUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression = TrueLiteral): LogicalPlan = {

    // plan.output may be narrowed (fewer columns than assignments) or reordered,
    // so assignments are matched by exprId.
    val assignmentMap = AttributeMap(assignments.collect {
      case Assignment(key: Attribute, value) => key -> value
    })

    val updatedValues = plan.output.map { attr =>
      if (MetadataAttribute.isValid(attr.metadata)) {
        if (MetadataAttribute.isPreservedOnUpdate(attr)) {
          attr
        } else {
          val updatedValue = If(cond, Literal(null, attr.dataType), attr)
          Alias(updatedValue, attr.name)(explicitMetadata = Some(attr.metadata))
        }
      } else {
        assignmentMap.get(attr) match {
          case Some(assignedExpr) =>
            Alias(If(cond, assignedExpr, attr), attr.name)()
          case None =>
            // Column in relation.output with no matching assignment; pass through unchanged.
            attr
        }
      }
    }

    val writeOp = If(cond, Literal(UPDATE_OPERATION), Literal(COPY_OPERATION))
    val operationCol = Alias(writeOp, OPERATION_COLUMN)()
    Project(operationCol +: updatedValues, plan)
  }

  // build a rewrite plan for sources that support row deltas
  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): WriteDelta = {

    val operation = operationTable.operation.asInstanceOf[SupportsDelta]
    val supportsColumnUpdate = operation.supportsColumnUpdates()

    val rowIdAttrs = resolveRowIdAttrs(relation, operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)

    val connectorDataAttrs = if (supportsColumnUpdate) {
      resolveRequiredDataAttrs(relation, operation)
    } else Nil

    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs, rowIdAttrs)

    // Connector-declared attrs not being assigned are passed through so ColumnPruning
    // keeps them in the scan and the connector receives their current values.
    val assignedAttrs = if (supportsColumnUpdate) computeAssignedAttrs(assignments)
                        else relation.output
    val connectorExtraAttrs: Seq[AttributeReference] = if (connectorDataAttrs.nonEmpty) {
      val assignedAttrSet = AttributeSet(assignedAttrs)
      connectorDataAttrs.filterNot(assignedAttrSet.contains)
    } else Nil

    // build a plan for updated records that match the condition
    val matchedRowsPlan = Filter(cond, readRelation)
    val rowDeltaPlan = if (operation.representUpdateAsDeleteAndInsert) {
      buildDeletesAndInserts(matchedRowsPlan, assignments, rowIdAttrs)
    } else if (supportsColumnUpdate) {
      buildColumnUpdateProjection(
        matchedRowsPlan, assignments, rowIdAttrs, metadataAttrs, connectorExtraAttrs)
    } else {
      buildWriteDeltaUpdateProjection(matchedRowsPlan, assignments, rowIdAttrs)
    }

    val effectiveRowAttrs = if (supportsColumnUpdate && connectorDataAttrs.nonEmpty) {
      connectorDataAttrs
    } else if (supportsColumnUpdate) {
      assignedAttrs
    } else {
      relation.output
    }

    // build a plan to write the row delta to the table
    val writeRelation = relation.copy(table = operationTable)
    val projections = buildWriteDeltaProjections(
      rowDeltaPlan, effectiveRowAttrs, rowIdAttrs, metadataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    WriteDelta(writeRelation, cond, rowDeltaPlan, relation, projections, groupFilterCond)
  }

  /**
   * Builds the WriteDelta projection for the column update path. The resulting Project
   * references only the columns needed for the write, so ColumnPruning narrows the scan
   * to match.
   */
  private def buildColumnUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute],
      connectorExtraAttrs: Seq[AttributeReference] = Nil): LogicalPlan = {

    val assignedValues = assignments.collect {
      case Assignment(key: Attribute, value) if !isIdentityAssignment(key, value) =>
        Alias(value, key.name)()
    }

    val connectorExtraAttrSet = AttributeSet(connectorExtraAttrs)
    val connectorPassThroughValues = plan.output.filter { a =>
      connectorExtraAttrSet.contains(a) && !MetadataAttribute.isValid(a.metadata)
    }

    val metadataAttrSet = AttributeSet(metadataAttrs)
    val metadataValues = plan.output.filter(metadataAttrSet.contains).map { attr =>
      if (MetadataAttribute.isPreservedOnUpdate(attr)) {
        attr
      } else {
        Alias(Literal(null, attr.dataType), attr.name)(explicitMetadata = Some(attr.metadata))
      }
    }

    val rowIdAttrSet = AttributeSet(rowIdAttrs)
    val rowIdValues = plan.output.filter(rowIdAttrSet.contains)

    val originalRowIdValues = buildOriginalRowIdValues(rowIdAttrs, assignments)
    val operationType = Alias(Literal(UPDATE_OPERATION), OPERATION_COLUMN)()

    Project(
      Seq(operationType) ++ assignedValues ++ connectorPassThroughValues ++
        metadataValues ++ rowIdValues ++ originalRowIdValues,
      plan)
  }

  // Returns the table attributes that are genuinely updated (non-identity) in this UPDATE.
  private def computeAssignedAttrs(assignments: Seq[Assignment]): Seq[AttributeReference] = {
    assignments.collect {
      case Assignment(key: AttributeReference, value) if !isIdentityAssignment(key, value) => key
    }
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
