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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, EqualNullSafe, Expression, If, Literal, MetadataAttribute, Not, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Expand, Filter, LogicalPlan, Project, ReplaceData, Union, UpdateTable, WriteDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.{RowLevelOperationTable, SupportsColumnUpdate, SupportsDelta}
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
    ReplaceData(writeRelation, cond, query, relation, projections, Some(cond))
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
    ReplaceData(writeRelation, cond, query, relation, projections, Some(cond))
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
    val supportsColumnUpdate = operation.isInstanceOf[SupportsColumnUpdate]

    if (supportsColumnUpdate && operation.representUpdateAsDeleteAndInsert) {
      logWarning(s"${operation.getClass.getSimpleName} implements SupportsColumnUpdate but " +
        s"also returns representUpdateAsDeleteAndInsert()=true; " +
        s"column-update optimization is disabled, full row will be sent to DeltaWriter.update")
    }

    // resolve all needed attrs (e.g. row ID and any required metadata attrs)
    val rowIdAttrs = resolveRowIdAttrs(relation, operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)

    // construct a read relation: for SupportsColumnUpdate, narrow the scan to only the columns
    // needed to evaluate assignment values and the filter condition, plus rowId and metadata.
    // This allows the connector's scan to project away unneeded columns from the data files.
    val readRelation =
      if (supportsColumnUpdate && !operation.representUpdateAsDeleteAndInsert) {
        val requiredScanAttrs =
          computeRequiredScanAttrs(relation, assignments, cond, rowIdAttrs, metadataAttrs)
        relation.copy(table = operationTable, output = requiredScanAttrs)
      } else {
        buildRelationWithAttrs(relation, operationTable, metadataAttrs, rowIdAttrs)
      }

    // build a plan for updated records that match the condition
    val matchedRowsPlan = Filter(cond, readRelation)
    val rowDeltaPlan = if (operation.representUpdateAsDeleteAndInsert) {
      buildDeletesAndInserts(matchedRowsPlan, assignments, rowIdAttrs)
    } else if (supportsColumnUpdate) {
      buildColumnUpdateProjection(matchedRowsPlan, assignments, rowIdAttrs)
    } else {
      buildWriteDeltaUpdateProjection(matchedRowsPlan, assignments, rowIdAttrs)
    }

    // when column update is supported, narrow rowAttrs to only changed columns so
    // LogicalWriteInfo.schema() carries a partial schema
    val effectiveRowAttrs =
      if (supportsColumnUpdate && !operation.representUpdateAsDeleteAndInsert) {
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

  // returns only the table attributes that are updated (changed) in this UPDATE;
  // identity assignments (SET col = col) are excluded. Implicit casts added by type resolution
  // are stripped before comparison. Two attributes are considered the same column if they share
  // the same ExprId (exact reference) or the same name and dataType (resolved from same table).
  private def computeAssignedAttrs(assignments: Seq[Assignment]): Seq[Attribute] = {
    assignments.collect {
      case Assignment(key: Attribute, value) if !isIdentityAssignment(key, value) => key
    }
  }

  // Computes the minimal set of attributes that must be read from the table for a column-update
  // path UPDATE. This includes:
  //   - table columns referenced in the values of SET assignments (right-hand sides)
  //   - table columns referenced in the WHERE condition
  //   - rowId attributes (needed to identify each row to the connector)
  //   - required metadata attributes (e.g. partition, index)
  // Columns that only appear as assignment targets (left-hand sides) and are not referenced
  // anywhere else are intentionally excluded; the connector receives their new values directly
  // from the projection, without needing to read the old values from storage.
  private def computeRequiredScanAttrs(
      relation: DataSourceV2Relation,
      assignments: Seq[Assignment],
      cond: Expression,
      rowIdAttrs: Seq[AttributeReference],
      metadataAttrs: Seq[AttributeReference]): Seq[AttributeReference] = {
    // only collect references from non-identity assignments; identity assignments (SET col = col)
    // do not need the old column value from storage since the new value equals the old value
    val assignmentValueRefs = assignments
      .filterNot {
        case Assignment(key: Attribute, value) => isIdentityAssignment(key, value)
        case _ => false
      }
      .flatMap(_.value.references.toSeq)
    val referencedExprIds =
      (assignmentValueRefs ++ cond.references.toSeq).map(_.exprId).toSet
    val referencedOutputAttrs =
      relation.output.filter(attr => referencedExprIds.contains(attr.exprId))
    // always include columns referenced by the table's partition transforms so that
    // V2ScanPartitioningAndOrdering can resolve the scan's outputPartitioning() against
    // the narrowed scan schema
    val partitionColNames = relation.table.partitioning()
      .flatMap(_.references().map(_.fieldNames.mkString(".")))
      .toSet
    val partitionAttrs = relation.output.filter(attr =>
      partitionColNames.exists(name => conf.resolver(attr.name, name)))
    // all inputs are AttributeReference; dedupAttrs returns Seq[Attribute] so cast is safe
    dedupAttrs(referencedOutputAttrs ++ partitionAttrs ++ rowIdAttrs ++ metadataAttrs)
      .map(_.asInstanceOf[AttributeReference])
  }

  // Builds the row delta projection for the SupportsColumnUpdate path. Unlike the full-row
  // buildWriteDeltaUpdateProjection, this method only emits genuinely-assigned (non-identity)
  // columns plus metadata passthrough. It does NOT require the scan to cover all table columns.
  private def buildColumnUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      rowIdAttrs: Seq[Attribute]): LogicalPlan = {
    // emit only the genuinely-changed columns (identity assignments are excluded)
    val updatedValues = assignments.collect {
      case Assignment(key: Attribute, value) if !isIdentityAssignment(key, value) =>
        Alias(value, key.name)()
    }
    // pass metadata attrs through from the (possibly narrow) plan output
    val metadataValues = plan.output.collect {
      case attr if MetadataAttribute.isValid(attr.metadata) =>
        if (MetadataAttribute.isPreservedOnUpdate(attr)) {
          attr
        } else {
          Alias(Literal(null, attr.dataType), attr.name)(explicitMetadata = Some(attr.metadata))
        }
    }
    val originalRowIdValues = buildOriginalRowIdValues(rowIdAttrs, assignments)
    // rowId attrs must appear in the projection output so that newLazyRowIdProjection can find
    // them via findColOrdinal; add them as plain pass-through unless they are already covered by
    // updatedValues (reassigned rowId) or originalRowIdValues (_original_ prefix variant)
    val updatedAttrNames = updatedValues.map(_.name).toSet
    val rowIdPassthrough = plan.output.filter { attr =>
      rowIdAttrs.exists(_.exprId == attr.exprId) && !updatedAttrNames.contains(attr.name)
    }
    // add any remaining columns from the narrow scan as passthroughs so that
    // PushDownUtils.pruneColumns retains them for the scan's outputPartitioning() reporting.
    // A column only needs a passthrough if it is NOT already a direct attribute reference in the
    // project. When it is a reference (e.g. salary in Alias(salary * 2, "salary")), the optimizer
    // already knows to keep it in the scan. When it is only an alias target for a literal or null
    // (e.g. dep in Alias(Literal('x'), "dep")), the reference is absent and the optimizer would
    // prune the column, breaking outputPartitioning() for partition columns like dep.
    val directlyReferencedExprIds =
      (updatedValues ++ metadataValues ++ rowIdPassthrough ++ originalRowIdValues)
        .flatMap(_.references).map(_.exprId).toSet
    val scanAnchorPassthrough = plan.output.filter { attr =>
      !MetadataAttribute.isValid(attr.metadata) &&
        !directlyReferencedExprIds.contains(attr.exprId)
    }
    val operationType = Alias(Literal(UPDATE_OPERATION), OPERATION_COLUMN)()
    Project(
      Seq(operationType) ++ updatedValues ++ metadataValues ++
        rowIdPassthrough ++ scanAnchorPassthrough ++ originalRowIdValues,
      plan)
  }

  private def isIdentityAssignment(key: Attribute, value: Expression): Boolean = {
    stripCasts(value) match {
      case attr: Attribute => attr.exprId == key.exprId
      case _ => false
    }
  }

  private def stripCasts(expr: Expression): Expression = expr match {
    case Cast(child, _, _, _) => stripCasts(child)
    case Alias(child, _) => stripCasts(child)
    case _ => expr
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
