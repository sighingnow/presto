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
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class VineyardPageSink
        implements ConnectorPageSink
{
    private final Logger log = Logger.get(VineyardPageSink.class);

    private final VineyardSession vineyardSession;
    private final VineyardOutputTableHandle tableHandle;
    private final List<VineyardColumnHandle> columns;

    private final List<Page> pending_pages;
    private final AtomicInteger pending_records;
    private final int LIMIT_FOR_CHUNKS = 100000;

    private final VineyardSession.TableBuilder tableBuilder;

    public VineyardPageSink(VineyardSession vineyardSession, VineyardOutputTableHandle tableHandle)
    {
        this.vineyardSession = requireNonNull(vineyardSession, "vineyard session is null");
        this.tableHandle = requireNonNull(tableHandle, "table handle is null");
        this.columns = new ArrayList<>(this.tableHandle.getColumns());

        this.pending_pages = new ArrayList<>();
        this.pending_records = new AtomicInteger(0);

        this.tableBuilder = vineyardSession.createTableBuilder(this.tableHandle.getTable().getTableName(), this.columns);
    }

    @Override
    public synchronized CompletableFuture<?> appendPage(Page page)
    {
        this.pending_pages.add(page);
        this.pending_records.getAndAdd(page.getPositionCount());

        this.flushPages(false);
        return CompletableFuture.completedFuture(ImmutableList.of());
    }

    @Override
    public synchronized CompletableFuture<Collection<Slice>> finish()
    {
        log.info("finish pages ...");

        this.flushPages(true);
        this.tableBuilder.finish();
        return CompletableFuture.completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        this.tableBuilder.abort();
    }

    private void flushPages(boolean force) {
        // pending
        if (!force && this.pending_records.get() < LIMIT_FOR_CHUNKS) {
            return;
        }

        if (this.pending_records.get() == 0) {
            return;
        }

        // generate underlying chunks
        val chunk = tableBuilder.createChunk(this.pending_records.get());

        log.info("size for the chunk: %d, total rows = %d",
                this.pending_pages.size(),
                this.pending_records.get());

        int total_offset = 0;
        for (int index = 0; index < this.pending_pages.size(); ++index) {
            val pending_page = this.pending_pages.get(index);

            for (int channel = 0; channel < pending_page.getChannelCount(); ++channel) {
                val block = pending_page.getBlock(channel);
                val builder = new PageColumnSink(chunk.getColumn(channel));
                builder.move(block, this.columns.get(channel).getColumnType(), total_offset);
            }

            total_offset += pending_page.getPositionCount();
        }

        tableBuilder.finishChunk(chunk);

        // mark as finished
        this.pending_pages.clear();
        this.pending_records.set(0);
    }
}
