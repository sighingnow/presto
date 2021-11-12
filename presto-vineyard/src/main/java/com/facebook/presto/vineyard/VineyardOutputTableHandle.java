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
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class VineyardOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final Logger log = Logger.get(VineyardOutputTableHandle.class);
    private final VineyardTableHandle table;
    private final Collection<VineyardColumnHandle> columns;

    @JsonCreator
    public VineyardOutputTableHandle(
            @JsonProperty("table") VineyardTableHandle table,
            @JsonProperty("columns") Collection<VineyardColumnHandle> columns)
    {
        log.info("outpout table: table = %s", table);
        this.table = requireNonNull(table, "table is null");
        this.columns = requireNonNull(columns, "table is null");
    }

    @JsonProperty
    public VineyardTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Collection<VineyardColumnHandle> getColumns()
    {
        return columns;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, columns);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        VineyardOutputTableHandle other = (VineyardOutputTableHandle) obj;
        return Objects.equals(this.table, other.table) &&
                Objects.equals(this.columns, other.columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("columns", columns)
                .toString();
    }
}
