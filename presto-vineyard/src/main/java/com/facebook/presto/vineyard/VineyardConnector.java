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

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import static com.facebook.presto.vineyard.VineyardTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class VineyardConnector
        implements Connector
{
    private static final Logger log = Logger.get(VineyardConnector.class);

    private final VineyardSession vineyardSession;

    private final LifeCycleManager lifeCycleManager;
    private final VineyardMetadata metadata;
    private final VineyardSplitManager splitManager;
    private final VineyardRecordSetProvider recordSetProvider;
    //    private final ArrowPageSourceProvider pageSourceProvider;
    private final VineyardPageSinkProvider pageSinkProvider;

    @Inject
    public VineyardConnector(
            VineyardSession vineyardSession,
            LifeCycleManager lifeCycleManager,
            VineyardMetadata metadata,
            VineyardSplitManager splitManager,
            VineyardRecordSetProvider recordSetProvider,
//            ArrowPageSourceProvider pageSourceProvider,
            VineyardPageSinkProvider pageSinkProvider)
    {
        this.vineyardSession = requireNonNull(vineyardSession, "vineyard session is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
//        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
    }

    public VineyardSession getVineyardSession()
    {
        return vineyardSession;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

//    @Override
//    public ConnectorPageSourceProvider getPageSourceProvider() {
//        return pageSourceProvider;
//    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
