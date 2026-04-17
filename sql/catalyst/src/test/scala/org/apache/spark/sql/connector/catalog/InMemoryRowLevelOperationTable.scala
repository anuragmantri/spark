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

package org.apache.spark.sql.connector.catalog

import java.time.Instant
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, NamedReference, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DeltaBatchWrite, DeltaWrite, DeltaWriteBuilder, DeltaWriter, DeltaWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, RequiresDistributionAndOrdering, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, SupportsDelta, Write, WriteBuilder, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

class InMemoryRowLevelOperationTable(
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    constraints: Array[Constraint] = Array.empty)
  extends InMemoryTable(
    name,
    CatalogV2Util.structTypeToV2Columns(schema),
    partitioning,
    properties,
    constraints)
  with SupportsRowLevelOperations {

  private final val PARTITION_COLUMN_REF = FieldReference(PartitionKeyColumn.name)
  private final val INDEX_COLUMN_REF = FieldReference(IndexColumn.name)
  private final val SUPPORTS_DELTAS = "supports-deltas"
  private final val SPLIT_UPDATES = "split-updates"
  private final val COLUMN_UPDATE = "column-update"
  private final val COLUMN_UPDATE_REQ_ATTRS = "column-update-req-attrs"
  // Selects PartitionBasedColumnUpdateOperation: CoW connector with supportsColumnUpdates=true
  // and requiredDataAttributes=[pk,dep].
  private final val COLUMN_UPDATE_COW = "column-update-cow"
  // Selects DeltaBasedColumnUpdateOperationFromInfo: connector that derives
  // requiredDataAttributes() dynamically from RowLevelOperationInfo.updatedColumns().
  // Always adds "pk" for row lookup plus whatever Spark reports as updated.
  private final val COLUMN_UPDATE_FROM_INFO = "column-update-from-info"
  // Selects DeltaBasedColumnUpdateSplitOperation: delta connector with
  // representUpdateAsDeleteAndInsert=true AND supportsColumnUpdates=true.
  // Used to verify Point 7: the restriction on column updates for the delete+reinsert path
  // has been lifted.
  private final val COLUMN_UPDATE_SPLIT = "column-update-split"

  // used in row-level operation tests to verify replaced partitions
  var replacedPartitions: Seq[Seq[Any]] = Seq.empty
  // used in row-level operation tests to verify reported write schema
  var lastWriteInfo: LogicalWriteInfo = _
  // used in column-update tests to verify the scan projection was narrowed correctly
  var lastScanSchema: StructType = _
  // used in column-update tests to verify that Spark passed the correct updated column list
  // to the connector via RowLevelOperationInfo.updatedColumns()
  var lastUpdatedColumns: Array[NamedReference] = Array.empty
  // used in row-level operation tests to verify passed records
  // (operation, id, metadata, row)
  var lastWriteLog: Seq[InternalRow] = Seq.empty

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    lastUpdatedColumns = info.updatedColumns()
    if (properties.getOrDefault(COLUMN_UPDATE, "false") == "true") {
      () => new DeltaBasedColumnUpdateOperation(info.command)
    } else if (properties.containsKey(COLUMN_UPDATE_REQ_ATTRS)) {
      val reqCols = properties.get(COLUMN_UPDATE_REQ_ATTRS).split(",").map(_.trim)
      () => new DeltaBasedColumnUpdateOperationWithReqAttrs(info.command, reqCols)
    } else if (properties.getOrDefault(COLUMN_UPDATE_FROM_INFO, "false") == "true") {
      () => new DeltaBasedColumnUpdateOperationFromInfo(info.command, info.updatedColumns().toSeq)
    } else if (properties.getOrDefault(COLUMN_UPDATE_COW, "false") == "true") {
      () => new PartitionBasedColumnUpdateOperation(info.command, info.updatedColumns().toSeq)
    } else if (properties.getOrDefault(COLUMN_UPDATE_SPLIT, "false") == "true") {
      () => new DeltaBasedColumnUpdateSplitOperation(info.command, info.updatedColumns().toSeq)
    } else if (properties.getOrDefault(SUPPORTS_DELTAS, "false") == "true") {
      () => DeltaBasedOperation(info.command)
    } else {
      () => PartitionBasedOperation(info.command)
    }
  }

  case class PartitionBasedOperation(command: Command) extends RowLevelOperation {
    var configuredScan: InMemoryBatchScan = _

    override def requiredMetadataAttributes(): Array[NamedReference] = {
      Array(PARTITION_COLUMN_REF, INDEX_COLUMN_REF)
    }

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new InMemoryScanBuilder(schema, options) {
        override def build: Scan = {
          val scan = super.build()
          configuredScan = scan.asInstanceOf[InMemoryBatchScan]
          scan
        }
      }
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      lastWriteInfo = info
      new WriteBuilder {
        override def build(): Write = new Write with RequiresDistributionAndOrdering {
          override def requiredDistribution: Distribution = {
            Distributions.clustered(Array(PARTITION_COLUMN_REF))
          }

          override def requiredOrdering: Array[SortOrder] = {
            Array[SortOrder](
              LogicalExpressions.sort(
                PARTITION_COLUMN_REF,
                SortDirection.ASCENDING,
                SortDirection.ASCENDING.defaultNullOrdering()))
          }

          override def toBatch: BatchWrite = PartitionBasedReplaceData(configuredScan)

          override def description: String = "InMemoryWrite"
        }
      }
    }

    override def description(): String = "InMemoryPartitionReplaceOperation"
  }

  abstract class RowLevelOperationBatchWrite extends TestBatchWrite {

    override def commit(messages: Array[WriterCommitMessage], metrics: WriteSummary): Unit = {
      commit(messages)
      commits += Commit(Instant.now().toEpochMilli, Some(metrics))
    }
  }

  private case class PartitionBasedReplaceData(scan: InMemoryBatchScan)
    extends RowLevelOperationBatchWrite {

    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      val readRows = scan.data.flatMap(_.asInstanceOf[BufferedRows].rows)
      val readPartitions = readRows.map(r => getKey(r, schema)).distinct
      dataMap --= readPartitions
      replacedPartitions = readPartitions
      withData(newData, schema)
      lastWriteLog = newData.flatMap(buffer => buffer.log).toImmutableArraySeq
    }
  }

  case class DeltaBasedOperation(command: Command) extends RowLevelOperation with SupportsDelta {
    private final val PK_COLUMN_REF = FieldReference("pk")

    override def requiredMetadataAttributes(): Array[NamedReference] = {
      Array(PARTITION_COLUMN_REF, INDEX_COLUMN_REF)
    }

    override def rowId(): Array[NamedReference] = Array(PK_COLUMN_REF)

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new InMemoryScanBuilder(schema, options)
    }

    override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder = {
      lastWriteInfo = info
      new DeltaWriteBuilder {
        override def build(): DeltaWrite = new DeltaWrite with RequiresDistributionAndOrdering {

          override def requiredDistribution(): Distribution = {
            Distributions.clustered(Array(PARTITION_COLUMN_REF))
          }

          override def requiredOrdering(): Array[SortOrder] = {
            Array[SortOrder](
              LogicalExpressions.sort(
                PARTITION_COLUMN_REF,
                SortDirection.ASCENDING,
                SortDirection.ASCENDING.defaultNullOrdering())
            )
          }

          override def toBatch: DeltaBatchWrite = TestDeltaBatchWrite
        }
      }
    }

    override def representUpdateAsDeleteAndInsert(): Boolean = {
      properties.getOrDefault(SPLIT_UPDATES, "false").toBoolean
    }
  }

  // A delta-based operation that supports column-level updates: Spark sends only the
  // assigned/changed columns in the row projection instead of the full row schema.
  class DeltaBasedColumnUpdateOperation(command: Command)
      extends DeltaBasedOperation(command) {
    override def representUpdateAsDeleteAndInsert(): Boolean = false
    override def supportsColumnUpdates(): Boolean = true

    // Override newScanBuilder to record the schema that Spark actually requests from the
    // connector after column pruning, so tests can assert on scan narrowing.
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new InMemoryScanBuilder(schema, options) {
        override def build(): Scan = {
          val scan = super.build()
          lastScanSchema = scan.readSchema()
          scan
        }
      }
    }

    override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder = {
      lastWriteInfo = info
      new DeltaWriteBuilder {
        override def build(): DeltaWrite =
          new DeltaWrite with RequiresDistributionAndOrdering {

            override def requiredDistribution(): Distribution = {
              Distributions.clustered(Array(PARTITION_COLUMN_REF))
            }

            override def requiredOrdering(): Array[SortOrder] = {
              Array[SortOrder](
                LogicalExpressions.sort(
                  PARTITION_COLUMN_REF,
                  SortDirection.ASCENDING,
                  SortDirection.ASCENDING.defaultNullOrdering())
              )
            }

            override def toBatch: DeltaBatchWrite =
              new RowLevelOperationBatchWrite with DeltaBatchWrite {
                override def createBatchWriterFactory(
                    info: PhysicalWriteInfo): DeltaWriterFactory = {
                  new DeltaBufferedRowsWriterFactory(lastWriteInfo.schema())
                }

                // For column-update writes, rows contain only the assigned columns
                // (narrow schema from LogicalWriteInfo). We expand each row to the full table
                // schema by overlaying write-schema columns on the base row found by pk.
                override def commit(messages: Array[WriterCommitMessage]): Unit =
                  dataMap.synchronized {
                    val newData = messages.map(_.asInstanceOf[BufferedRows])
                    val writeSchema = lastWriteInfo.schema()
                    val writeFieldIdx = writeSchema.fieldNames.zipWithIndex.toMap

                    val mergedData = newData.map { buf =>
                      val merged = new BufferedRows(buf.key, schema)
                      val updateOpName = UTF8String.fromString(Update.toString)
                      buf.log.foreach { logRow =>
                        val opName = logRow.getUTF8String(0)
                        if (opName == updateOpName) {
                          val pk = logRow.getInt(1)
                          val narrowRow = logRow.get(3, writeSchema).asInstanceOf[InternalRow]
                          val baseRow = dataMap.values.iterator.flatten
                            .flatMap(_.rows)
                            .find(r => r.getInt(0) == pk)
                          val fullRow = new GenericInternalRow(schema.length)
                          baseRow.foreach { base =>
                            for (i <- schema.fields.indices) {
                              fullRow.update(i, base.get(i, schema(i).dataType))
                            }
                          }
                          schema.fields.zipWithIndex.foreach { case (field, i) =>
                            writeFieldIdx.get(field.name).foreach { j =>
                              fullRow.update(i, narrowRow.get(j, field.dataType))
                            }
                          }
                          merged.rows.append(fullRow)
                        }
                      }
                      merged
                    }

                    withDeletes(newData)
                    withData(mergedData)
                    lastWriteLog = newData.flatMap(buffer => buffer.log).toIndexedSeq
                  }

                override def abort(messages: Array[WriterCommitMessage]): Unit = {}
              }
          }
      }
    }
  }

  // A variant of DeltaBasedColumnUpdateOperation that overrides requiredDataAttributes()
  // to declare a fixed set of data columns the connector needs in the scan.  This exercises
  // the connector-driven scan-narrowing path (as opposed to the heuristic path).
  class DeltaBasedColumnUpdateOperationWithReqAttrs(command: Command, reqCols: Array[String])
      extends DeltaBasedColumnUpdateOperation(command) {
    override def requiredDataAttributes(): Array[NamedReference] = reqCols.map(FieldReference(_))
  }

  // A delta-based column-update connector that derives requiredDataAttributes() dynamically
  // from RowLevelOperationInfo.updatedColumns().
  //
  // This models the common connector pattern:
  //   1. Spark tells the connector which columns are being updated via updatedColumns().
  //   2. The connector adds any extra columns it always needs (here: "pk" for row lookup).
  //   3. The combined set is returned from requiredDataAttributes() so Spark narrows the scan.
  //
  // If "pk" is already in updatedColumns (the user is updating pk itself), it is not duplicated.
  class DeltaBasedColumnUpdateOperationFromInfo(
      command: Command,
      updatedCols: Seq[NamedReference])
      extends DeltaBasedColumnUpdateOperation(command) {

    private val PK_REF: NamedReference = FieldReference("pk")

    override def requiredDataAttributes(): Array[NamedReference] = {
      val updatedNames = updatedCols.map(_.describe()).toSet
      if (updatedNames.contains("pk")) {
        updatedCols.toArray
      } else {
        (Array(PK_REF) ++ updatedCols).toArray
      }
    }
  }

  // A delta-based operation that combines representUpdateAsDeleteAndInsert=true with
  // supportsColumnUpdates()=true.  This verifies that the restriction which previously
  // blocked column-level updates on the delete+reinsert path has been lifted.
  //
  // The connector declares "pk" plus any columns being updated (via updatedCols).
  // The write schema = requiredDataAttributes() in declared order.
  // The REINSERT leg receives the narrow write row; the DELETE leg uses row ID only.
  class DeltaBasedColumnUpdateSplitOperation(
      command: Command,
      updatedCols: Seq[NamedReference] = Nil)
      extends DeltaBasedColumnUpdateOperation(command) {
    override def representUpdateAsDeleteAndInsert(): Boolean = true

    private val PK_REF: NamedReference = FieldReference("pk")
    override def requiredDataAttributes(): Array[NamedReference] = {
      val updatedNames = updatedCols.map(_.describe()).toSet
      if (updatedNames.contains("pk")) updatedCols.toArray
      else (Array(PK_REF) ++ updatedCols).toArray
    }

    override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder = {
      lastWriteInfo = info
      new DeltaWriteBuilder {
        override def build(): DeltaWrite =
          new DeltaWrite with RequiresDistributionAndOrdering {
            override def requiredDistribution(): Distribution =
              Distributions.clustered(Array(PARTITION_COLUMN_REF))
            override def requiredOrdering(): Array[SortOrder] = Array[SortOrder](
              LogicalExpressions.sort(
                PARTITION_COLUMN_REF,
                SortDirection.ASCENDING,
                SortDirection.ASCENDING.defaultNullOrdering()))
            override def toBatch: DeltaBatchWrite =
              new RowLevelOperationBatchWrite with DeltaBatchWrite {
                override def createBatchWriterFactory(
                    info: PhysicalWriteInfo): DeltaWriterFactory =
                  new DeltaBufferedRowsWriterFactory(lastWriteInfo.schema())

                // For delete+reinsert with narrow writes, the REINSERT row has only the
                // connector-declared columns (requiredDataAttributes order).
                // pk is the first field in the write schema (declared before updatedCols).
                // Reconstruct the full row by overlaying the narrow row onto the original.
                override def commit(messages: Array[WriterCommitMessage]): Unit =
                  dataMap.synchronized {
                    val newData = messages.map(_.asInstanceOf[BufferedRows])
                    val writeSchema = lastWriteInfo.schema()
                    val writeFieldIdx = writeSchema.fieldNames.zipWithIndex.toMap
                    val reinsertOpName = UTF8String.fromString(Reinsert.toString)
                    val pkIdx = writeFieldIdx("pk")

                    val expandedData = newData.map { buf =>
                      val expanded = new BufferedRows(buf.key, schema)
                      buf.log.foreach { logRow =>
                        val opName = logRow.getUTF8String(0)
                        if (opName == reinsertOpName) {
                          val narrowRow = logRow.get(3, writeSchema).asInstanceOf[InternalRow]
                          val pk = narrowRow.getInt(pkIdx)
                          val baseRow = dataMap.values.iterator.flatten
                            .flatMap(_.rows)
                            .find(r => r.getInt(0) == pk)
                          val fullRow = new GenericInternalRow(schema.length)
                          baseRow.foreach { base =>
                            for (i <- schema.fields.indices) {
                              fullRow.update(i, base.get(i, schema(i).dataType))
                            }
                          }
                          schema.fields.zipWithIndex.foreach { case (field, i) =>
                            writeFieldIdx.get(field.name).foreach { j =>
                              fullRow.update(i, narrowRow.get(j, field.dataType))
                            }
                          }
                          expanded.rows.append(fullRow)
                        }
                      }
                      expanded
                    }

                    withDeletes(newData)
                    withData(expandedData)
                    lastWriteLog = newData.flatMap(buffer => buffer.log).toIndexedSeq
                  }

                override def abort(messages: Array[WriterCommitMessage]): Unit = {}
              }
          }
      }
    }
  }

  // A CoW operation that supports column-level updates.  The connector declares it needs
  // "pk" and "dep" for partition routing, plus any columns the user is updating (via
  // updatedCols from RowLevelOperationInfo).  supportsColumnUpdates()=true so Spark narrows
  // the scan and write schema to exactly requiredDataAttributes().
  // The commit logic reconstructs full rows from the original scan data using pk as a key.
  class PartitionBasedColumnUpdateOperation(
      command: Command,
      updatedCols: Seq[NamedReference] = Nil) extends RowLevelOperation {
    var configuredScan: InMemoryBatchScan = _

    override def command(): Command = command

    override def supportsColumnUpdates(): Boolean = true

    override def requiredDataAttributes(): Array[NamedReference] = {
      // Always need pk (for row lookup) and dep (partition key).
      // Also include any columns being updated so Spark sends their new values.
      val base = Seq(FieldReference("pk"), FieldReference("dep"))
      val baseNames = base.map(_.describe()).toSet
      (base ++ updatedCols.filterNot(r => baseNames.contains(r.describe()))).toArray
    }

    override def requiredMetadataAttributes(): Array[NamedReference] =
      Array(PARTITION_COLUMN_REF, INDEX_COLUMN_REF)

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new InMemoryScanBuilder(schema, options) {
        override def build(): Scan = {
          val scan = super.build()
          configuredScan = scan.asInstanceOf[InMemoryBatchScan]
          lastScanSchema = scan.readSchema()
          scan
        }
      }
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      lastWriteInfo = info
      new WriteBuilder {
        override def build(): Write = new Write with RequiresDistributionAndOrdering {
          override def requiredDistribution: Distribution =
            Distributions.clustered(Array(PARTITION_COLUMN_REF))

          override def requiredOrdering: Array[SortOrder] = Array[SortOrder](
            LogicalExpressions.sort(
              PARTITION_COLUMN_REF,
              SortDirection.ASCENDING,
              SortDirection.ASCENDING.defaultNullOrdering()))

          override def toBatch: BatchWrite =
            PartitionBasedNarrowReplaceData(configuredScan, info.schema())

          override def description: String = "InMemoryNarrowCoWWrite"
        }
      }
    }

    override def description(): String = "InMemoryPartitionColumnUpdateOperation"
  }

  // CoW write handler for narrow column-update writes.
  // Receives rows with only the connector-declared + assigned columns.
  // Reconstructs full rows by looking up the original row by pk and overlaying received columns.
  private case class PartitionBasedNarrowReplaceData(
      scan: InMemoryBatchScan,
      writeSchema: StructType) extends RowLevelOperationBatchWrite {

    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      val readRows = scan.data.flatMap(_.asInstanceOf[BufferedRows].rows)
      val readPartitions = readRows.map(r => getKey(r, schema)).distinct
      dataMap --= readPartitions
      replacedPartitions = readPartitions

      val writeFieldIdx = writeSchema.fieldNames.zipWithIndex.toMap
      val pkIdxInWrite = writeFieldIdx("pk")
      val pkIdxInFull = schema.fieldIndex("pk")

      val expandedData = newData.map { buf =>
        val expanded = new BufferedRows(buf.key, schema)
        buf.rows.foreach { narrowRow =>
          val pk = narrowRow.getInt(pkIdxInWrite)
          val origRow = readRows.find(r => r.getInt(pkIdxInFull) == pk)
          val fullRow = new GenericInternalRow(schema.length)
          origRow.foreach { base =>
            for (i <- schema.fields.indices) {
              fullRow.update(i, base.get(i, schema(i).dataType))
            }
          }
          schema.fields.zipWithIndex.foreach { case (field, i) =>
            writeFieldIdx.get(field.name).foreach { j =>
              fullRow.update(i, narrowRow.get(j, field.dataType))
            }
          }
          expanded.rows.append(fullRow)
        }
        expanded
      }

      withData(expandedData, schema)
      lastWriteLog = newData.flatMap(buffer => buffer.log).toImmutableArraySeq
    }
  }

  private object TestDeltaBatchWrite extends RowLevelOperationBatchWrite with DeltaBatchWrite{
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory = {
      new DeltaBufferedRowsWriterFactory(CatalogV2Util.v2ColumnsToStructType(columns()))
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      withDeletes(newData)
      withData(newData, columns())
      lastWriteLog = newData.flatMap(buffer => buffer.log).toIndexedSeq
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  }
}

private class DeltaBufferedRowsWriterFactory(schema: StructType) extends DeltaWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DeltaWriter[InternalRow] = {
    new DeltaBufferWriter(schema)
  }
}

private class DeltaBufferWriter(schema: StructType) extends BufferWriter(schema)
  with DeltaWriter[InternalRow] {

  private final val DELETE = UTF8String.fromString(Delete.toString)
  private final val UPDATE = UTF8String.fromString(Update.toString)
  private final val REINSERT = UTF8String.fromString(Reinsert.toString)
  private final val INSERT = UTF8String.fromString(Insert.toString)

  override def delete(meta: InternalRow, id: InternalRow): Unit = {
    val pk = id.getInt(0)
    buffer.deletes += pk
    val logEntry = new GenericInternalRow(Array[Any](DELETE, pk, meta.copy(), null))
    buffer.log += logEntry
  }

  override def update(meta: InternalRow, id: InternalRow, row: InternalRow): Unit = {
    val pk = id.getInt(0)
    buffer.deletes += pk
    buffer.rows.append(row.copy())
    val logEntry = new GenericInternalRow(Array[Any](UPDATE, pk, meta.copy(), row.copy()))
    buffer.log += logEntry
  }

  override def reinsert(meta: InternalRow, row: InternalRow): Unit = {
    buffer.rows.append(row.copy())
    val logEntry = new GenericInternalRow(Array[Any](REINSERT, null, meta.copy(), row.copy()))
    buffer.log += logEntry
  }

  override def insert(row: InternalRow): Unit = {
    buffer.rows.append(row.copy())
    val logEntry = new GenericInternalRow(Array[Any](INSERT, null, null, row.copy()))
    buffer.log += logEntry
  }

  override def write(row: InternalRow): Unit = {
    throw new UnsupportedOperationException()
  }

  override def commit(): WriterCommitMessage = buffer
}
