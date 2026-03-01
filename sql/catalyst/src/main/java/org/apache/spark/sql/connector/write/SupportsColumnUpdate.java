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

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Experimental;

/**
 * A mix-in interface for {@link SupportsDelta}. Data sources can implement this interface
 * to indicate they support column-level update writes.
 * <p>
 * When a connector implements this interface, Spark will only include the assigned/changed
 * columns in the row projection passed to {@link DeltaWriter#update}, rather than the full
 * table row. The write schema reported via {@link LogicalWriteInfo#schema()} will contain
 * only the assigned columns, allowing the connector to identify which columns are present
 * and write a column file accordingly.
 * <p>
 * Implementations of this interface MUST return {@code false} from
 * {@link SupportsDelta#representUpdateAsDeleteAndInsert()}, as the split delete-and-insert
 * path requires a full row and is incompatible with column-only writes. Spark skips the
 * column-update optimization whenever {@code representUpdateAsDeleteAndInsert()} returns
 * {@code true}.
 * <p>
 * When all SET assignments in an UPDATE are identity assignments (e.g. {@code SET a = a}),
 * Spark will call {@link DeltaWriter#update} with an empty {@code InternalRow} (zero fields)
 * as the row argument. Implementations must handle this case gracefully, treating it as a
 * no-op column update (no columns need to be written).
 *
 * @since 4.1.0
 */
@Experimental
public interface SupportsColumnUpdate extends SupportsDelta {
}
