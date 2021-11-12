/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.vineyard;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class VineyardRecordSet
        implements RecordSet
{
    private final Logger log = Logger.get(VineyardSplit.class);

    private final VineyardSession vineyardSession;
    private final List<VineyardColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final String tableName;
    private final String tablePath;
    private final int splitIndex;

    public VineyardRecordSet(
            VineyardSession vineyardSession,
            VineyardSplit split,
            List<VineyardColumnHandle> columnHandles)
    {
        this.vineyardSession = vineyardSession;
        requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (VineyardColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        this.tableName = split.getTableName();
        this.tablePath = split.getTablePath();
        this.splitIndex = split.getSplitIndex();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new VineyardRecordCursor(
                vineyardSession, columnHandles, tableName, tablePath, splitIndex);
    }
}
