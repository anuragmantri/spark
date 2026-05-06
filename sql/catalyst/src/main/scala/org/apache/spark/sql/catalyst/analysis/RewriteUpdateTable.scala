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
            case Assignment(key: AttributeReference, value)
                if !isIdentityAssignment(key, value) =>
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

    val (readRelation, rowAttrs) = buildCoWReadSetup(relation, operationTable, assignments, cond)

    val updatedAndRemainingRowsPlan = buildReplaceDataUpdateProjection(
      readRelation, assignments, cond)

    val writeRelation = relation.copy(table = operationTable)
    val query = updatedAndRemainingRowsPlan
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)
    val projections = buildReplaceDataProjections(query, rowAttrs, metadataAttrs)
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

    val (readRelation, rowAttrs) = buildCoWReadSetup(relation, operationTable, assignments, cond)

    // build a plan for updated records that match the condition
    val matchedRowsPlan = Filter(cond, readRelation)
    val updatedRowsPlan = buildReplaceDataUpdateProjection(matchedRowsPlan, assignments)

    // build a plan that contains unmatched rows in matched groups that must be copied over
    val remainingRowFilter = Not(EqualNullSafe(cond, Literal.TrueLiteral))
    val remainingRowsPlan = addOperationColumn(COPY_OPERATION,
      Filter(remainingRowFilter, readRelation))

    val updatedAndRemainingRowsPlan = Union(updatedRowsPlan, remainingRowsPlan)

    val writeRelation = relation.copy(table = operationTable)
    val query = updatedAndRemainingRowsPlan
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)
    val projections = buildReplaceDataProjections(query, rowAttrs, metadataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    ReplaceData(writeRelation, cond, query, relation, projections, groupFilterCond)
  }

  // Common read-relation setup shared by both CoW plan builders.
  //
  // When the connector supports column updates and declares required data attributes,
  // the read relation is narrowed at analysis time so that
  // GroupBasedRowLevelOperationScanPlanning uses only the needed columns for the scan.
  // Otherwise the full relation output is used.
  private def buildCoWReadSetup(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): (DataSourceV2Relation, Seq[Attribute]) = {

    val operation = operationTable.operation
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)
    val connectorDataAttrs = resolveRequiredDataAttrs(relation, operation)
    val isNarrow = operation.supportsColumnUpdates() && connectorDataAttrs.nonEmpty

    // CoW scan narrowing must be done manually at analysis time.
    // GroupBasedRowLevelOperationScanPlanning (an optimizer rule that fires after analysis)
    // always reads relation.output directly when building the physical scan -- it does not
    // observe Project nodes above the relation, so optimizer-driven column pruning has no
    // effect on CoW scans.  We narrow DataSourceV2Relation.output here so that rule picks
    // up the narrow set.
    val readRelation = if (isNarrow) {
      val allRequired = (connectorDataAttrs ++ computeAssignedAttrs(assignments)).distinct
      buildRelationWithAttrs(relation, operationTable, metadataAttrs, dataAttrs = allRequired,
        cond = cond)
    } else {
      buildRelationWithAttrs(relation, operationTable, metadataAttrs)
    }

    // CoW write schema (two paths only, no heuristic for CoW):
    // - Narrow path (connectorDataAttrs declared): exactly connector-declared cols in declared
    //   order.  The connector must declare ALL columns it wants to receive.
    // - Full path (connectorDataAttrs empty OR supportsColumnUpdates=false): full table output.
    //   Unlike MOR, CoW does not have a heuristic assigned-only path because
    //   GroupBasedRowLevelOperationScanPlanning needs explicit column declarations to narrow.
    val rowAttrs: Seq[Attribute] = if (isNarrow) connectorDataAttrs else relation.output

    (readRelation, rowAttrs)
  }

  // this method assumes the assignments have been already aligned before
  //
  // Works for both the full-scan and narrow-scan CoW paths.  In the narrow case,
  // readRelation.output is already restricted by buildCoWReadSetup, so projecting
  // all plan.output gives the correct narrow write schema.
  private def buildReplaceDataUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression = TrueLiteral): LogicalPlan = {

    // Build a name-keyed map via AttributeMap (compares by exprId internally) so we can look
    // up each plan column's assignment without relying on positional ordering.  This is more
    // robust than position-based indexing and works correctly for any plan output layout.
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
            // Column is present in the scan but has no assignment -- pass through unchanged.
            // In the narrow CoW path these are connector-declared columns not being updated.
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
    // Column-update support applies to the standard delta path and the delete+reinsert path.
    // When representUpdateAsDeleteAndInsert is true, the REINSERT leg of the Expand already
    // uses only assigned values, so the narrow effectiveRowAttrs applies correctly.
    val supportsColumnUpdate = operation.supportsColumnUpdates()

    // resolve all needed attrs (e.g. row ID and any required metadata attrs)
    val rowIdAttrs = resolveRowIdAttrs(relation, operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)

    // Connector-declared data attrs used to determine pass-through columns in the write plan.
    val connectorDataAttrs = if (supportsColumnUpdate) {
      resolveRequiredDataAttrs(relation, operation)
    } else Nil

    // MOR uses a full-schema scan; ColumnPruning narrows it via Project references.
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs, rowIdAttrs)

    // Connector-required attrs that are NOT being assigned are added as pass-throughs in the
    // plan so that ColumnPruning keeps them in the physical scan AND the connector receives
    // their current values via DeltaWriter.update's row argument.
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

    // Effective row write schema:
    // - Narrow path (connectorDataAttrs declared): exactly connector-declared cols in declared
    //   order.  The connector must declare ALL columns it wants to receive (including updated
    //   ones).  This mirrors the metadata pattern and enables strict areCompatible validation.
    // - Heuristic path (connectorDataAttrs empty): only the assigned (changed) columns.
    // - Full path (no column-update support): full table output.
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

  // Builds the row delta projection for the column update path.
  //
  // The resulting Project references only:
  //   - assigned column values (new values being written)
  //   - connector pass-through values (connector declared but not assigned)
  //   - metadata columns (nulled or preserved)
  //   - row ID columns (for delta identification)
  //   - original row ID values (only when a row ID column is being reassigned)
  //
  // ColumnPruning observes exactly these references and narrows the physical scan accordingly.
  // Connectors that need additional columns in the scan (e.g., partition columns for
  // distribution) should declare them in requiredDataAttributes().
  //
  // Note: AlignUpdateAssignments guarantees all assignment keys are top-level
  // AttributeReferences even for nested field updates (e.g., SET col1.field = 'x' becomes
  // Assignment(col1: AttributeReference, CreateNamedStruct(...))), so isIdentityAssignment
  // correctly identifies non-updating assignments.
  private def buildColumnUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute],
      connectorExtraAttrs: Seq[AttributeReference] = Nil): LogicalPlan = {

    // only emit values for non-identity assignments (the narrow write schema)
    val assignedValues = assignments.collect {
      case Assignment(key: Attribute, value) if !isIdentityAssignment(key, value) =>
        Alias(value, key.name)()
    }

    // Connector-required data attrs that are not being assigned are passed through as-is
    // so that (a) ColumnPruning keeps them in the physical scan, and (b) the connector
    // receives their current values via DeltaWriter.update's row argument.
    val connectorExtraAttrSet = AttributeSet(connectorExtraAttrs)
    val connectorPassThroughValues = plan.output.filter { a =>
      connectorExtraAttrSet.contains(a) && !MetadataAttribute.isValid(a.metadata)
    }

    // pass through or null out metadata columns present in the scan
    val metadataAttrSet = AttributeSet(metadataAttrs)
    val metadataValues = plan.output.filter(metadataAttrSet.contains).map { attr =>
      if (MetadataAttribute.isPreservedOnUpdate(attr)) {
        attr
      } else {
        Alias(Literal(null, attr.dataType), attr.name)(explicitMetadata = Some(attr.metadata))
      }
    }

    // pass through row ID columns from the scan
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

  private def isIdentityAssignment(key: Attribute, value: Expression): Boolean = {
    val unwrapped = value match {
      case Alias(child, _) => child
      case other => other
    }
    unwrapped match {
      case attr: Attribute => AttributeSet(Seq(key)).contains(attr)
      case _ => false
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
