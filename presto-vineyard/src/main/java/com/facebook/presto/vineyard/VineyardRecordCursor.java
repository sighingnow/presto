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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.v6d.modules.basic.columnar.ColumnarData;
import lombok.SneakyThrows;
import lombok.val;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class VineyardRecordCursor
        implements RecordCursor
{
    private final Logger log = Logger.get(VineyardRecordCursor.class);

    private final VineyardSession vineyardSession;
    private final List<VineyardColumnHandle> columnHandles;
    private final String tableName;
    private final int[] fieldToColumnIndex;

    private final ColumnarData[] table;
    private final int totalLoc;
    private int currentLoc;

    @SneakyThrows(IOException.class)
    public VineyardRecordCursor(
            final VineyardSession vineyardSession,
            final List<VineyardColumnHandle> columnHandles,
            final String tableName,
            final String tablePath,
            final int splitIndex)
    {
        this.vineyardSession = requireNonNull(vineyardSession, "vineyard session is null");
        this.columnHandles = requireNonNull(columnHandles, "columns handle is null");
        this.tableName = requireNonNull(tableName, "table name is null");

        val columns = vineyardSession.loadSplit(tablePath, splitIndex);

        this.fieldToColumnIndex = new int[columnHandles.size()];
        this.table = new ColumnarData[this.columnHandles.size()];
        for (int index = 0; index < columnHandles.size(); index++) {
            VineyardColumnHandle columnHandle = columnHandles.get(index);
            this.fieldToColumnIndex[index] = columnHandle.getColumnIndex();
            this.table[index] = columns.get(columnHandle.getColumnIndex());
        }

        this.totalLoc = this.table[0].valueCount();
        this.currentLoc = -1;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (this.currentLoc >= this.totalLoc - 1 /* note the `-1` here */) {
            return false;
        }
        this.currentLoc += 1;
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BooleanType.BOOLEAN);
        return this.table[field].getBoolean(currentLoc);
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BigintType.BIGINT);
        return this.table[field].getLong(currentLoc);
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DoubleType.DOUBLE);
        return this.table[field].getDouble(currentLoc);
    }

    @Override
    public Slice getSlice(int field)
    {
        val type = getType(field);
        checkArgument(isVarcharType(getType(field)), "expect var char type");
        val text = this.table[field].getUTF8String(currentLoc);
        return Slices.wrappedBuffer(text.getBytes(), 0, text.getLength());
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return table[field].isNullAt(currentLoc);
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
