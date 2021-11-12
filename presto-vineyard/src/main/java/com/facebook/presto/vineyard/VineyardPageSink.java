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
import com.facebook.presto.common.Page;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.vineyard.impl.PageColumnSink;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import lombok.val;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class VineyardPageSink
        implements ConnectorPageSink
{
    private final Logger log = Logger.get(VineyardPageSink.class);

    private final VineyardSession vineyardSession;
    private final VineyardOutputTableHandle tableHandle;
    private final List<VineyardColumnHandle> columns;

    private final VineyardSession.TableBuilder tableBuilder;

    public VineyardPageSink(VineyardSession vineyardSession, VineyardOutputTableHandle tableHandle)
    {
        this.vineyardSession = requireNonNull(vineyardSession, "vineyard session is null");
        this.tableHandle = requireNonNull(tableHandle, "table handle is null");
        this.columns = new ArrayList<>(this.tableHandle.getColumns());

        for (val column : this.columns) {
            log.info("output table: column = %s", column);
        }
        this.tableBuilder = vineyardSession.createTableBuilder(this.tableHandle.getTable().getTableName(), this.columns);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        log.info("page = %s", page);
        log.info("append page: %s: channel count = %d, position count = %d", page, page.getChannelCount(), page.getPositionCount());

        val chunk = tableBuilder.createChunk(page.getPositionCount());

        for (int channel = 0; channel < page.getChannelCount(); ++channel) {
            val block = page.getBlock(channel);
            log.info("block is: %s, %s", block.getClass(), block);
            val builder = new PageColumnSink(chunk.getColumn(channel));
            builder.move(block, this.columns.get(channel).getColumnType());
        }
        tableBuilder.finishChunk(chunk);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        log.info("finish pages ...");
        tableBuilder.finish();
        return CompletableFuture.completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }
}
