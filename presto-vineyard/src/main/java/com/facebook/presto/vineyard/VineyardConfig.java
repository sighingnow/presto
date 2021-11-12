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

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import static com.google.common.base.MoreObjects.toStringHelper;

public class VineyardConfig
{
    private String mode;
    private String vineyardSocket;
    private String arrowRoot;

    @NotNull
    public String getMode()
    {
        return mode;
    }

    @Config("mode")
    public VineyardConfig setMode(String mode)
    {
        this.mode = mode;
        return this;
    }

    @NotNull
    public String getVineyardSocket()
    {
        return vineyardSocket;
    }

    @Config("vineyard-socket")
    public VineyardConfig setVineyardSocket(String vineyardSocket)
    {
        this.vineyardSocket = vineyardSocket;
        return this;
    }

    @NotNull
    public String getArrowRoot()
    {
        return arrowRoot;
    }

    @Config("arrow-root")
    public VineyardConfig setArrowRoot(String arrowRoot)
    {
        this.arrowRoot = arrowRoot;
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("mode", mode).add("arrow-root", arrowRoot).add("vineyard-socket", vineyardSocket).toString();
    }
}
