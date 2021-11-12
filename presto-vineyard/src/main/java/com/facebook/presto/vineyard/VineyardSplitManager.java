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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import lombok.val;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class VineyardSplitManager
        implements ConnectorSplitManager
{
    private final Logger log = Logger.get(VineyardSplitManager.class);

    private final String connectorId;
    private final NodeManager nodeManager;

    private final VineyardSession vineyardSession;

    @Inject
    public VineyardSplitManager(VineyardConnectorId connectorId, NodeManager nodeManager, VineyardSession vineyardSession)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.vineyardSession = requireNonNull(vineyardSession, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle handle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        VineyardTableLayoutHandle layoutHandle = (VineyardTableLayoutHandle) layout;
        VineyardTableHandle tableHandle = layoutHandle.getTable();
        VineyardTable table = vineyardSession.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        log.info("current node = %s, workers = %s, nodes: %s",
                nodeManager.getCurrentNode(),
                nodeManager.getWorkerNodes(),
                nodeManager.getAllNodes());

        val node = nodeManager.getCurrentNode();

        val tablePath = vineyardSession.getTablePath(tableHandle.getSchemaName(), tableHandle.getTableName());
        val tableSplits = vineyardSession.getTableSplits(tableHandle.getSchemaName(), tableHandle.getTableName());
        List<ConnectorSplit> splits = new ArrayList<>();
        for (val tableSplit : tableSplits.entrySet()) {
            splits.add(new VineyardSplit(
                    connectorId, node.getHostAndPort(),
                    tableHandle.getSchemaName(),
                    tableHandle.getTableName(),
                    tablePath,
                    tableSplit.getKey()));
        }
//        List<ConnectorSplit> splits = nodeManager.getAllNodes().stream()
//                .map(node -> new ArrowSplit(connectorId, node.getHostAndPort(), tableHandle.getSchemaName(), tableHandle.getTableName(), tableHandle.getTablePath()))
//                .collect(Collectors.toList());
        return new FixedSplitSource(splits);
    }
}
