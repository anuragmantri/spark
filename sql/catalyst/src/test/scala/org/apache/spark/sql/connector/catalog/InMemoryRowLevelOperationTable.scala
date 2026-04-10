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

  // used in row-level operation tests to verify replaced partitions
  var replacedPartitions: Seq[Seq[Any]] = Seq.empty
  // used in row-level operation tests to verify reported write schema
  var lastWriteInfo: LogicalWriteInfo = _
  // used in row-level operation tests to verify passed records
  // (operation, id, metadata, row)
  var lastWriteLog: Seq[InternalRow] = Seq.empty

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    if (properties.getOrDefault(COLUMN_UPDATE, "false") == "true") {
      () => new DeltaBasedColumnUpdateOperation(info.command)
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
  // representUpdateAsDeleteAndInsert must be false -- the split path requires a full row.
  class DeltaBasedColumnUpdateOperation(command: Command)
      extends DeltaBasedOperation(command) {
    override def representUpdateAsDeleteAndInsert(): Boolean = false
    override def supportsColumnUpdates(): Boolean = true

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

                // For column-update writes, rows in the buffer contain only the assigned columns
                // (narrow schema from LogicalWriteInfo). We must expand each row to the full table
                // schema before inserting into the in-memory table so that getKey() works
                // correctly.
                override def commit(messages: Array[WriterCommitMessage]): Unit =
                  dataMap.synchronized {
                    val newData = messages.map(_.asInstanceOf[BufferedRows])
                    val writeSchema = lastWriteInfo.schema()
                    val writeFieldIdx = writeSchema.fieldNames.zipWithIndex.toMap

                    // For each updated row, read the existing full row from the dataMap by pk,
                    // then overlay only the columns present in the narrow write schema.
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
