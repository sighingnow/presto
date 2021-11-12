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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.vineyard.impl.arrow.ArrowClient;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class VineyardRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final Logger log = Logger.get(ArrowClient.class);
    private final VineyardSession vineyardSession;
    private final String connectorId;

    @Inject
    public VineyardRecordSetProvider(VineyardSession vineyardSession, VineyardConnectorId connectorId)
    {
        this.vineyardSession = requireNonNull(vineyardSession, "vineyard session is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "partitionChunk is null");
        VineyardSplit exampleSplit = (VineyardSplit) split;

        ImmutableList.Builder<VineyardColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((VineyardColumnHandle) handle);
        }

        return new VineyardRecordSet(vineyardSession, exampleSplit, handles.build());
    }
}
