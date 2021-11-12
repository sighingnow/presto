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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.vineyard.impl.arrow.ArrowClient;
import com.facebook.presto.vineyard.impl.vineyard.VineyardClient;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.v6d.core.common.util.VineyardException;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

public class VineyardModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;

    public VineyardModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Singleton
    @Provides
    public static VineyardSession createVineyardSession(VineyardConfig config, TypeManager typeManager, NodeManager nodeManager)
            throws VineyardException
    {
        if ("arrow".equals(config.getMode())) {
            return new ArrowClient(config, nodeManager);
        }
        if ("vineyard".equals(config.getMode())) {
            return new VineyardClient(config, nodeManager);
        }
        throw new VineyardException.Invalid("vineyard config is invalid: " + config);
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(VineyardConnector.class).in(Scopes.SINGLETON);
        binder.bind(VineyardConnectorId.class).toInstance(new VineyardConnectorId(connectorId));
        binder.bind(VineyardMetadata.class).in(Scopes.SINGLETON);
        binder.bind(VineyardSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(VineyardRecordSetProvider.class).in(Scopes.SINGLETON);
//        binder.bind(ArrowPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(VineyardPageSinkProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(VineyardConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(VineyardTable.class));
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(parseTypeSignature(value));
        }
    }
}
