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
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static java.util.Objects.requireNonNull;

public class VineyardSplit
        implements ConnectorSplit
{
    private final Logger log = Logger.get(VineyardSplit.class);

    private final String connectorId;
    private final HostAddress address;
    private final String schemaName;
    private final String tableName;
    private final String tablePath;
    private final int splitIndex;

    @JsonCreator
    public VineyardSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("address") HostAddress address,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tablePath") String tablePath,
            @JsonProperty("splitIndex") int splitIndex)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.address = requireNonNull(address, "node address is null");
        ;
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.tablePath = requireNonNull(tablePath, "table path is null");
        this.splitIndex = requireNonNull(splitIndex, "table path is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(address);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getTablePath()
    {
        return tablePath;
    }

    @JsonProperty
    public int getSplitIndex()
    {
        return splitIndex;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return HARD_AFFINITY;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return ImmutableList.of(address);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
