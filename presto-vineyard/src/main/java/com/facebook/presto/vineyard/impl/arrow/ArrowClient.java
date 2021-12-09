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
package com.facebook.presto.vineyard.impl.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.vineyard.VineyardConfig;
import com.facebook.presto.vineyard.VineyardSession;
import com.facebook.presto.vineyard.VineyardTable;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import io.v6d.core.common.util.VineyardException;
import io.v6d.modules.basic.arrow.Arrow;
import io.v6d.modules.basic.columnar.ColumnarData;
import io.v6d.modules.basic.columnar.ColumnarDataBuilder;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class ArrowClient
        extends VineyardSession
{
    private final Logger log = Logger.get(ArrowClient.class);
    private static final String SCHEMA_NAME = "arrow";

    private BufferAllocator allocator;
    private ConcurrentMap<String, SeekableReadChannel> channels;
    private ConcurrentMap<String, ArrowFileReader> readers;

    public ArrowClient(VineyardConfig config, NodeManager manager)
    {
        super(config, manager);

        log.info("initializing arrow client");

        this.allocator = new RootAllocator();
        this.schemas = Suppliers.memoize(schemasSupplier(config.getArrowRoot()));

        this.channels = new ConcurrentSkipListMap<>();
        this.readers = new ConcurrentSkipListMap<>();
    }

    @Override
    public String getTablePath(String schema, String tableName)
    {
        return config.getArrowRoot() + "/" + tableName + ".arrow";
    }

    @Override
    public int getSplitSize(String schema, String tableName)
    {
        return getTableSplits(schema, tableName).size();
    }

    @Override
    @SneakyThrows({FileNotFoundException.class, IOException.class})
    public Map<Integer, HostAddress> getTableSplits(String schema, String tableName)
    {
        long timeUsage = 0;
        timeUsage -= System.currentTimeMillis();

        val tablePath = getTablePath(schema, tableName);
        val channel = new FileInputStream(tablePath).getChannel();
        val reader = new ArrowFileReader(channel, allocator);
        this.channels.put(tablePath, new SeekableReadChannel(channel));
        this.readers.put(tablePath, reader);
        val splits = ImmutableMap.<Integer, HostAddress>builder();
        for (int index = 0; index < reader.getRecordBlocks().size(); ++index) {
            splits.put(index, manager.getCurrentNode().getHostAndPort());
        }

        timeUsage += System.currentTimeMillis();
        log.info("[timing][arrow]: initializing tables splits for %s use %d", tablePath, timeUsage);

        return splits.build();
    }

    private Supplier<Map<String, Map<String, VineyardTable>>> schemasSupplier
            (String arrowRoot)
    {
        return () -> {
            try {
                return readSchema(arrowRoot);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private Map<String, Map<String, VineyardTable>> readSchema(
            String arrowRoot)
            throws IOException
    {
        long timeUsage = 0;
        val schema = new HashMap<String, Map<String, VineyardTable>>();
        val tables = new HashMap<String, VineyardTable>();

        timeUsage -= System.currentTimeMillis();
        File file = new File(arrowRoot);
        String[] arrowfiles = file.list(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".arrow");
            }
        });
        for (val arrowfile : arrowfiles) {
            val tablePath = arrowRoot + "/" + arrowfile;
            val tableName = arrowfile.substring(0, arrowfile.length() - ".arrow".length());
            try {
                val reader = new ArrowFileReader(new FileInputStream(tablePath).getChannel(), allocator);
                val table = reader.getVectorSchemaRoot();

                tables.put(tableName, buildTableSchemaFromArrowSchema(table.getSchema(), tableName, tablePath));
            }
            catch (IOException e) {
                log.error("Failed to read the schema from arrow file: %s", e);
                throw new UncheckedIOException(e);
            }
        }

        timeUsage += System.currentTimeMillis();
        log.info("[timing][arrow]: initializing tables use %d", timeUsage);

        schema.put(SCHEMA_NAME, tables);
        return ImmutableMap.copyOf(schema);
    }

    @SneakyThrows(IOException.class)
    private VectorSchemaRoot loadRecordBatch(final String tablePath, int splitIndex)
    {
        val reader = readers.get(tablePath);
        val table = VectorSchemaRoot.create(reader.getVectorSchemaRoot().getSchema(), Arrow.default_allocator);
        val loader = new VectorLoader(table);

        val block = reader.getRecordBlocks().get(splitIndex);
        // create a new channel
        try (val channel = new SeekableReadChannel(new FileInputStream(tablePath).getChannel())) {
            channel.setPosition(block.getOffset());
            val batch = MessageSerializer.deserializeRecordBatch(channel, block, Arrow.default_allocator);
            loader.load(batch);
        }
        return table;
    }

    @Override
    public List<ColumnarData> loadSplit(String tablePath, int splitIndex)
            throws IOException
    {
        long timeUsage = 0;
        if (!this.channels.containsKey(tablePath) || !this.readers.containsKey(tablePath)) {
            log.error("reader not found for %s", tablePath);
            throw new IOException("reader not found: " + tablePath);
        }
        timeUsage -= System.currentTimeMillis();
        val table = this.loadRecordBatch(tablePath, splitIndex);
        timeUsage += System.currentTimeMillis();
        log.info("[timing][arrow]: load split for %d use %d", splitIndex, timeUsage);

        return table.getFieldVectors().stream().filter(field -> !skipType(field.getField().getType())).map(field -> {
            return new ColumnarData(field);
        }).collect(Collectors.toList());
    }

    @Override
    public VineyardSession.TableBuilder createTableBuilder(String tableName, Schema schema)
    {
        return new ArrowTableBuilder(tableName, schema);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void dropTable(String schemaName, String tableName)
    {
        assert schemaName.equals(SCHEMA_NAME);
        val tablePath = getTablePath(schemaName, tableName);
        if (!readers.containsKey(tablePath)) {
            throw new IndexOutOfBoundsException("table '" + tableName + "' doesn't exist: " + tablePath);
        }

        val reader = readers.get(tablePath);
        reader.close();
        readers.remove(tablePath);

        val channel = channels.get(tablePath);
        channel.close();
        channels.remove(tablePath);

        val tables = schemas.get().get(SCHEMA_NAME);
        tables.remove(tableName);

        new File(tablePath).delete();
    }

    public class ArrowChunkBuilder
            extends ChunkBuilder
    {
        private final List<FieldVector> vectors;
        private final List<ColumnarDataBuilder> builders;

        public ArrowChunkBuilder(Schema schema, int rows)
        {
            this.vectors = schema.getFields().stream().map(field -> {
                val vector = builderForField(field);
                vector.setValueCount(rows);
                return vector;
            }).collect(Collectors.toList());
            this.builders = this.vectors.stream().map(vector -> {
                return new ColumnarDataBuilder(vector);
            }).collect(Collectors.toList());
        }

        public ArrowChunkBuilder(Schema schema, int rows, List<FieldVector> vectors)
        {
            this.vectors = vectors;
            this.builders = this.vectors.stream().map(vector -> {
                return new ColumnarDataBuilder(vector);
            }).collect(Collectors.toList());
        }

        @SneakyThrows(VineyardException.class)
        private FieldVector builderForField(Field field)
        {
            FieldVector vector;
            if (field.getType().equals(Arrow.Type.Int)) {
                vector = new IntVector(field, Arrow.default_allocator);
            }
            else if (field.getType().equals(Arrow.Type.Int64)) {
                vector = new BigIntVector(field, Arrow.default_allocator);
            }
            else if (field.getType().equals(Arrow.Type.Float)) {
                vector = new Float4Vector(field, Arrow.default_allocator);
            }
            else if (field.getType().equals(Arrow.Type.Double)) {
                vector = new Float8Vector(field, Arrow.default_allocator);
            }
            else if (field.getType().equals(Arrow.Type.VarChar)) {
                vector = new LargeVarCharVector(field, Arrow.default_allocator);
            }
            else if (field.getType().equals(Arrow.Type.VarBinary)) {
                vector = new LargeVarBinaryVector(field, Arrow.default_allocator);
            }
            else {
                throw new VineyardException.NotImplemented(
                        "array builder for type " + field.getType() + " is not supported");
            }
            return vector;
        }

        @Override
        public List<ColumnarDataBuilder> getColumns()
        {
            return this.builders;
        }

        @Override
        public ColumnarDataBuilder getColumn(int index)
        {
            return this.builders.get(index);
        }
    }

    public class ArrowTableBuilder
            extends TableBuilder
    {
        private long timeUsage = 0;
        private final String tablePath;
        private final List<FieldVector> vectors;
        private final VectorSchemaRoot root;
        private final FileOutputStream output;
        private final ArrowWriter writer;

        @SneakyThrows(IOException.class)
        public ArrowTableBuilder(String tableName, Schema schema)
        {
            super(tableName, schema);
            this.tablePath = config.getArrowRoot() + "/" + tableName + ".arrow";
            this.vectors = schema.getFields().stream().map(field -> field.createVector(Arrow.default_allocator)).collect(Collectors.toList());
            this.root = new VectorSchemaRoot(this.vectors);
            this.output = new FileOutputStream(tablePath);
            if (this.tableName.endsWith("_stream")) {
                this.writer = new ArrowStreamWriter(root, null, this.output.getChannel());
            } else {
                this.writer = new ArrowFileWriter(root, null, this.output.getChannel());
            }

            timeUsage -= System.currentTimeMillis();
            // start the writter
            this.writer.start();
            timeUsage += System.currentTimeMillis();
        }

        @Override
        public ChunkBuilder createChunk(int rows)
        {
            val fields = this.root.getFieldVectors();
            for (val field : fields) {
                field.reset();
            }
            this.root.setRowCount(rows);  // nb. setRowCount is required
            return new ArrowChunkBuilder(schema, rows, this.root.getFieldVectors());
        }

        @Override
        @SneakyThrows(IOException.class)
        public void finishChunk(ChunkBuilder builder)
        {
            timeUsage -= System.currentTimeMillis();
            this.writer.writeBatch();
            this.output.flush();
            timeUsage += System.currentTimeMillis();
        }

        @Override
        @SneakyThrows(IOException.class)
        public void finish()
        {
            timeUsage -= System.currentTimeMillis();
            log.info("finishing writing arrow files");
            this.output.flush();
            this.writer.close();

            val tables = schemas.get().get(SCHEMA_NAME);
            tables.put(tableName, buildTableSchemaFromArrowSchema(schema, tableName, tablePath));

            timeUsage += System.currentTimeMillis();
            log.info("[timing][arrow]: create-then-finish tables use %d", timeUsage);
        }

        @Override
        @SneakyThrows(IOException.class)
        public void abort()
        {
            this.output.flush();
            this.writer.close();

            new File(tablePath).delete();
        }
    }
}
