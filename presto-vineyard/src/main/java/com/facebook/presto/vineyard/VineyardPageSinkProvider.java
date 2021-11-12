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
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.v6d.core.common.util.VineyardException;
import lombok.SneakyThrows;
import lombok.val;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class VineyardPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final Logger log = Logger.get(VineyardPageSinkProvider.class);

    private final VineyardConfig config;
    private final VineyardSession vineyardSession;

    @Inject
    public VineyardPageSinkProvider(VineyardConfig config, VineyardSession vineyardSession)
    {
        this.config = requireNonNull(config, "config is null");
        this.vineyardSession = requireNonNull(vineyardSession, "vineyard session is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkContext pageSinkContext)
    {
        log.info("create page sink from output table handle");
        val handle = (VineyardOutputTableHandle) outputTableHandle;
        return new VineyardPageSink(vineyardSession, handle);
    }

    @Override
    @SneakyThrows(VineyardException.class)
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkContext pageSinkContext)
    {
        log.error("page sink: not implemented for insert table handle");
        throw new VineyardException.NotImplemented("Not implemented for insert table handle");
    }
}
