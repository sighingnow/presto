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
package com.facebook.presto.vineyard.impl.vineyard;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.vineyard.VineyardConfig;
import com.facebook.presto.vineyard.VineyardSession;
import com.facebook.presto.vineyard.VineyardTable;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import io.v6d.core.client.IPCClient;
import io.v6d.core.client.ds.ObjectFactory;
import io.v6d.core.common.util.ObjectID;
import io.v6d.core.common.util.VineyardException;
import io.v6d.modules.basic.arrow.Arrow;
import io.v6d.modules.basic.arrow.RecordBatch;
import io.v6d.modules.basic.arrow.RecordBatchBuilder;
import io.v6d.modules.basic.arrow.SchemaBuilder;
import io.v6d.modules.basic.arrow.Table;
import io.v6d.modules.basic.columnar.ColumnarData;
import io.v6d.modules.basic.columnar.ColumnarDataBuilder;
import io.v6d.modules.basic.dataframe.DataFrame;
import io.v6d.modules.basic.stream.DataFrameStream;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class VineyardClient
        extends VineyardSession
{
    private static final String SCHEMA_NAME = "vineyard";
    private Logger log = Logger.get(VineyardClient.class);
    private final IPCClient client;
    private ConcurrentMap<String, Table> batches;
    private Cache<String, Map<Integer, HostAddress>> tableSplits;

    public VineyardClient(VineyardConfig config, NodeManager manager)
            throws VineyardException
    {
        super(config, manager);

        log.info("initializing vineyard client");

        this.client = new IPCClient(config.getVineyardSocket());
        this.batches = new ConcurrentSkipListMap<>();
        this.schemas = Suppliers.memoize(schemasSupplier(this.client));
        this.tableSplits = CacheBuilder.newBuilder().build();
    }

    private Supplier<Map<String, Map<String, VineyardTable>>> schemasSupplier
            (IPCClient client)
    {
        return () -> {
            try {
                return readSchema(client);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (VineyardException e) {
                throw new UncheckedIOException(new IOException(e.getMessage()));
            }
        };
    }

    private Map<String, Map<String, VineyardTable>> readSchema(IPCClient client)
            throws IOException, VineyardException
    {
        long timeUsage = 0;
        val schema = new HashMap<String, Map<String, VineyardTable>>();
        val tables = new HashMap<String, VineyardTable>();

        timeUsage -= System.currentTimeMillis();
        val dataframes = client.listMetaData("vineyard::Table", false, Integer.MAX_VALUE);

        for (val meta : dataframes) {
            Table table;
            try {
                table = (Table) ObjectFactory.getFactory().resolve(meta);
            }
            catch (Exception e) {
                log.error(e);
                continue;
            }
            val id = table.getId().toString();
            val prestoTable = buildTableSchemaFromArrowSchema(table.getSchema().getSchema(), id, id);
            this.batches.put(id, table);
            tables.put(id, prestoTable);
            if (table.getMeta().hasName()) {
                val name = table.getMeta().getName();
                this.batches.put(name, table);
                tables.put(name, prestoTable);
            }
        }

        timeUsage += System.currentTimeMillis();
        log.info("[timing][vineyard]: initializing tables use %d", timeUsage);

        schema.put(SCHEMA_NAME, tables);
        return ImmutableMap.copyOf(schema);
    }

    @Override
    public String getTablePath(String schema, String tableName)
    {
        return tableName;
    }

    @Override
    public int getSplitSize(String schema, String tableName)
    {
        return getTableSplits(schema, tableName).size();
    }

    @Override
    @SneakyThrows(ExecutionException.class)
    public Map<Integer, HostAddress> getTableSplits(String schema, String tableName)
    {
        val tablePath = getTablePath(schema, tableName);
        return tableSplits.get(tablePath, new Callable<Map<Integer, HostAddress>>()
        {
            @Override
            public Map<Integer, HostAddress> call()
            {
                val table = batches.get(tablePath);
                val splits = new ConcurrentSkipListMap<Integer, HostAddress>();
                for (int index = 0; index < table.getBatches().size(); ++index) {
                    splits.put(index, manager.getCurrentNode().getHostAndPort());
                }
                return splits;
            }
        });
    }

    @Override
    public List<ColumnarData> loadSplit(String tablePath, int splitIndex)
            throws IOException
    {
        long timeUsage = 0;
        if (!this.batches.containsKey(tablePath)) {
            log.error("reader not found for %s", tablePath);
            throw new IOException("reader not found: " + tablePath);
        }
        timeUsage -= System.currentTimeMillis();
        val table = this.batches.get(tablePath);
        val batch = table.getBatch(splitIndex).getBatch();

        timeUsage += System.currentTimeMillis();
        log.info("[timing][vineyard]: load split for %d use %d", splitIndex, timeUsage);

        return batch.getFieldVectors().stream().filter(field -> !skipType(field.getField().getType())).map(field -> {
            return new ColumnarData(field);
        }).collect(Collectors.toList());
    }

    @Override
    public VineyardSession.TableBuilder createTableBuilder(String tableName, Schema schema)
    {
        return new VineyardTableBuilder(tableName, schema);
    }

    @Override
    @SneakyThrows(VineyardException.class)
    public void dropTable(String schemaName, String tableName)
    {
        assert schemaName.equals(SCHEMA_NAME);
        if (!batches.containsKey(tableName)) {
            throw new VineyardException.ObjectNotExists("table '" + tableName + "' doesn't exist");
        }
        val tables = schemas.get().get(SCHEMA_NAME);
        val batch = batches.get(tableName);
        if (batch.getMeta().hasName()) {
            String name = batch.getMeta().getName();
            ObjectID id = batch.getMeta().getId();

            client.delete(Collections.singletonList(id));
            tables.remove(name);

            this.batches.remove(id.toString());
            tables.remove(id.toString());

            this.batches.remove(name);
            client.dropName(name);
        }
        else {
            ObjectID id = batch.getMeta().getId();
            client.delete(Collections.singletonList(id));
            this.batches.remove(id.toString());
            tables.remove(id.toString());
        }
    }

    public class VineyardChunkBuilder
            extends ChunkBuilder
    {
        private final RecordBatchBuilder builder;

        public VineyardChunkBuilder(IPCClient client, Schema schema, int rows)
                throws VineyardException
        {
            this.builder = new RecordBatchBuilder(client, schema, rows);
        }

        @Override
        @SneakyThrows(VineyardException.class)
        public List<ColumnarDataBuilder> getColumns()
        {
            return builder.getColumnBuilders();
        }

        @Override
        @SneakyThrows(VineyardException.class)
        public ColumnarDataBuilder getColumn(int index)
        {
            return builder.getColumnBuilder(index);
        }

        public RecordBatchBuilder getBatch()
        {
            return builder;
        }

        @SneakyThrows(VineyardException.class)
        private void dumper(int rows)
        {
            if (rows <= 0) {
                return;
            }
            if (rows > builder.getColumnBuilder(0).valueCount()) {
                rows = builder.getColumnBuilder(0).valueCount();
            }
            val buffer = new StringBuilder();
            buffer.append("dumping table as:");
            for (int index = 0; index < rows; ++index) {
                for (val column : builder.getColumnBuilders()) {
                    buffer.append(column.getObject(index));
                    buffer.append(",");
                }
                buffer.append("\n");
            }
            System.out.println(buffer);
        }
    }

    private String stripStreamSuffix(String tableName) {
        if (tableName.endsWith("_stream")) {
            return tableName.substring(0, tableName.length() - "_stream".length());
        } else {
            return tableName;
        }
    }

    public class VineyardTableBuilder
            extends TableBuilder
    {
        private long timeUsage = 0;
        private final io.v6d.modules.basic.arrow.TableBuilder tableBuilder;

        private final DataFrameStream stream;

        @SneakyThrows(VineyardException.class)
        public VineyardTableBuilder(String tableName, Schema schema)
        {
            super(stripStreamSuffix(tableName), schema);
            this.tableBuilder = new io.v6d.modules.basic.arrow.TableBuilder(client, SchemaBuilder.fromSchema(schema));

            if (tableName.endsWith("_stream")) {
                this.stream = DataFrameStream.create(client);
                client.persist(this.stream.getId());
                client.putName(this.stream.getId(), tableName);
            } else {
                stream = null;
            }
        }

        @Override
        @SneakyThrows(VineyardException.class)
        public ChunkBuilder createChunk(int rows)
        {
            return new VineyardChunkBuilder(client, schema, rows);
        }

        @Override
        @SneakyThrows(VineyardException.class)
        public void finishChunk(ChunkBuilder builder)
        {
            timeUsage -= System.currentTimeMillis();
            val chunk = (VineyardChunkBuilder) builder;
            val meta = chunk.getBatch().seal(client);
            val batch = (RecordBatch) ObjectFactory.getFactory().resolve(meta);
            tableBuilder.addBatch(batch);
            if (this.stream != null) {
                stream.writer().append(meta.getId());
            }
            timeUsage += System.currentTimeMillis();
        }

        @Override
        @SneakyThrows(VineyardException.class)
        public void finish()
        {
            timeUsage -= System.currentTimeMillis();
            val meta = tableBuilder.seal(client);
            log.info("create vineyard table: %s, object is %s", tableName, meta.getId());

            val id = meta.getId().toString();
            val table = (Table) ObjectFactory.getFactory().resolve(meta);
            val prestoTable = buildTableSchemaFromArrowSchema(table.getSchema().getSchema(), id, id);

            client.persist(meta);
            client.putName(meta.getId(), tableName);

            if (this.stream != null) {
                this.stream.writer().finish();
            }

            val tables = schemas.get().get(SCHEMA_NAME);
            batches.put(id, table);
            tables.put(id, prestoTable);
            batches.put(tableName, table);
            tables.put(tableName, prestoTable);

            timeUsage += System.currentTimeMillis();

            log.info("[timing][vineyard]: create-then-finish tables use %d", timeUsage);
        }

        @Override
        @SneakyThrows(VineyardException.class)
        public void abort()
        {
            this.stream.writer().fail();
        }
    }

    static {
        Arrow.instantiate();
    }
}
