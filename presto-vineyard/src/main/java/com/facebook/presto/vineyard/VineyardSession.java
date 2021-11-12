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
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.v6d.modules.basic.arrow.Arrow;
import io.v6d.modules.basic.columnar.ColumnarData;
import io.v6d.modules.basic.columnar.ColumnarDataBuilder;
import lombok.val;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public abstract class VineyardSession
{
    private static final Logger log = Logger.get(VineyardSession.class);

    protected VineyardConfig config;
    protected NodeManager manager;

    protected Supplier<Map<String, Map<String, VineyardTable>>> schemas;

    public VineyardSession(VineyardConfig config, NodeManager manager)
    {
        this.config = requireNonNull(config, "config is null");
        this.manager = requireNonNull(manager, "config is null");
    }

    protected static VineyardTable buildTableSchemaFromArrowSchema(Schema schema, String tableName, String tablePath)
    {
        log.debug("build table schema: %s", schema);
        val columns = new ArrayList<VineyardColumn>();
        val fields = schema.getFields();
        for (int index = 0; index < fields.size(); ++index) {
            val field = fields.get(index);
            if (skipType(field.getType())) {
                continue;
            }
            val column = new VineyardColumn(field.getName(), fromArrowType(field.getType()));
            columns.add(column);
        }
        return new VineyardTable(tableName, tablePath, columns);
    }

    protected static Schema buildTableSchemaFromColumnHandles(Collection<VineyardColumnHandle> columnHandles)
    {
        log.debug("build output table schema: %s", columnHandles);
        List<Field> fields = columnHandles.stream().map(column -> {
            return Arrow.makeField(column.getColumnName(), toArrowFieldType(column.getColumnType()));
        }).collect(Collectors.toList());
        val metadata = ImmutableMap.of("created-by", "presto");
        return new Schema(Collections2.immutableListCopy(fields), metadata);
    }

    protected static boolean skipType(ArrowType type)
    {
        if (type.getTypeID() == ArrowType.ArrowTypeID.Null) {
            return true;
        }
        return false;
    }

    protected static Type fromArrowType(ArrowType type)
    {
        if (type.equals(Arrow.Type.Boolean)) {
            return BooleanType.BOOLEAN;
        }
        if (type.equals(Arrow.Type.Int) || type.equals(Arrow.Type.UInt)) {
            return IntegerType.INTEGER;
        }
        if (type.equals(Arrow.Type.Int64) || type.equals(Arrow.Type.UInt64)) {
            return BigintType.BIGINT;
        }
        if (type.equals(Arrow.Type.Double)) {
            return DoubleType.DOUBLE;
        }
        if (type.getTypeID() == ArrowType.ArrowTypeID.Utf8) {
            return VarcharType.VARCHAR;
        }
        if (type.getTypeID() == ArrowType.ArrowTypeID.LargeUtf8) {
            return VarcharType.VARCHAR;
        }
        if (type.getTypeID() == ArrowType.ArrowTypeID.Binary) {
            return VarbinaryType.VARBINARY;
        }
        if (type.getTypeID() == ArrowType.ArrowTypeID.LargeBinary) {
            return VarbinaryType.VARBINARY;
        }
        if (type.getTypeID() == ArrowType.ArrowTypeID.Date) {
            return DateType.DATE;
        }
        if (type.getTypeID() == ArrowType.ArrowTypeID.Time) {
            return TimeType.TIME;
        }
        if (type.getTypeID() == ArrowType.ArrowTypeID.Timestamp) {
            return TimestampType.TIMESTAMP;
        }
        throw new PrestoException(NOT_SUPPORTED, "not support arrow datatype: " + type);
    }

    protected static ArrowType toArrowType(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return Arrow.Type.Boolean;
        }
        if (type.equals(IntegerType.INTEGER)) {
            return Arrow.Type.Int;
        }
        if (type.equals(BigintType.BIGINT)) {
            return Arrow.Type.Int64;
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return Arrow.Type.Double;
        }
        if (type.equals(VarcharType.VARCHAR) || isVarcharType(type)) {
            return Arrow.Type.VarChar;
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return Arrow.Type.VarBinary;
        }
        throw new PrestoException(NOT_SUPPORTED, "not support presto datatype: " + type);
    }

    protected static FieldType toArrowFieldType(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return Arrow.FieldType.Boolean;
        }
        if (type.equals(IntegerType.INTEGER)) {
            return Arrow.FieldType.Int;
        }
        if (type.equals(BigintType.BIGINT)) {
            return Arrow.FieldType.Int64;
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return Arrow.FieldType.Double;
        }
        if (type.equals(VarcharType.VARCHAR) || isVarcharType(type)) {
            return Arrow.FieldType.VarChar;
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return Arrow.FieldType.VarBinary;
        }
        throw new PrestoException(NOT_SUPPORTED, "not support presto datatype: " + type);
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        Map<String, VineyardTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public VineyardTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, VineyardTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    public abstract String getTablePath(String schema, String tableName);

    public abstract int getSplitSize(String schema, String tableName);

    public abstract Map<Integer, HostAddress> getTableSplits(String schema, String tableName);

    public Collection<ColumnarData> loadSplit(String schema, String tableName, int splitIndex)
            throws IOException
    {
        return loadSplit(getTablePath(schema, tableName), splitIndex);
    }

    public abstract List<ColumnarData> loadSplit(String tablePath, int splitIndex)
            throws IOException;

    public abstract TableBuilder createTableBuilder(String tableName, Schema schema);

    public TableBuilder createTableBuilder(String tableName, Collection<VineyardColumnHandle> columns)
    {
        return createTableBuilder(tableName, buildTableSchemaFromColumnHandles(columns));
    }

    public abstract void dropTable(String schemaName, String tableName);

    public abstract class ChunkBuilder
    {
        public abstract List<ColumnarDataBuilder> getColumns();

        public abstract ColumnarDataBuilder getColumn(int index);
    }

    public abstract class TableBuilder
    {
        protected final String tableName;
        protected final Schema schema;

        public TableBuilder(String tableName, Schema schema)
        {
            this.tableName = requireNonNull(tableName, "schema for table builder is null");
            this.schema = requireNonNull(schema, "schema for table builder is null");
        }

        public abstract ChunkBuilder createChunk(int rows);

        public abstract void finishChunk(ChunkBuilder builder);

        public abstract void finish();
    }
}
