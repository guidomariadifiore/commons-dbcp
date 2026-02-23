/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.dbcp2.datasources;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Collections; // Required for Collections.list
import java.util.Enumeration; // Required for Enumeration

import javax.naming.RefAddr;
import javax.naming.Reference;

/**
 * A JNDI ObjectFactory which creates {@link SharedPoolDataSource}s
 *
 * @since 2.0
 */
public class PerUserPoolDataSourceFactory extends InstanceKeyDataSourceFactory {
    private static final String PER_USER_POOL_CLASSNAME = PerUserPoolDataSource.class.getName();

    /**
     * Constructs a new instance.
     */
    public PerUserPoolDataSourceFactory() {
        // empty
    }

    @SuppressWarnings("unchecked") // Avoid warnings on deserialization
    @Override
    protected InstanceKeyDataSource getNewInstance(final Reference ref) throws IOException, ClassNotFoundException {
        final PerUserPoolDataSource pupds = new PerUserPoolDataSource();

        final Enumeration<RefAddr> allRefAddrs = ref.getAll();
        if (allRefAddrs != null) {
            for (final RefAddr refAddr : Collections.list(allRefAddrs)) {
                if (refAddr != null && refAddr.getContent() != null) {
                    final String type = refAddr.getType();
                    switch (type) {
                        case "defaultMaxTotal":
                            pupds.setDefaultMaxTotal(parseInt(refAddr));
                            break;
                        case "defaultMaxIdle":
                            pupds.setDefaultMaxIdle(parseInt(refAddr));
                            break;
                        case "defaultMaxWaitMillis":
                            pupds.setDefaultMaxWait(Duration.ofMillis(parseInt(refAddr)));
                            break;
                        case "perUserDefaultAutoCommit":
                            final byte[] serialized = (byte[]) refAddr.getContent();
                            pupds.setPerUserDefaultAutoCommit((Map<String, Boolean>) deserialize(serialized));
                            break;
                        case "perUserDefaultTransactionIsolation":
                            final byte[] serialized1 = (byte[]) refAddr.getContent(); // Using distinct variable names for block scope
                            pupds.setPerUserDefaultTransactionIsolation((Map<String, Integer>) deserialize(serialized1));
                            break;
                        case "perUserMaxTotal":
                            final byte[] serialized2 = (byte[]) refAddr.getContent();
                            pupds.setPerUserMaxTotal((Map<String, Integer>) deserialize(serialized2));
                            break;
                        case "perUserMaxIdle":
                            final byte[] serialized3 = (byte[]) refAddr.getContent();
                            pupds.setPerUserMaxIdle((Map<String, Integer>) deserialize(serialized3));
                            break;
                        case "perUserMaxWaitMillis":
                            final byte[] serialized4 = (byte[]) refAddr.getContent();
                            pupds.setPerUserMaxWaitMillis((Map<String, Long>) deserialize(serialized4));
                            break;
                        case "perUserDefaultReadOnly":
                            final byte[] serialized5 = (byte[]) refAddr.getContent();
                            pupds.setPerUserDefaultReadOnly((Map<String, Boolean>) deserialize(serialized5));
                            break;
                        // No default case is added to strictly maintain the original logical behavior
                        // which silently ignores unrecognized RefAddr types.
                    }
                }
            }
        }
        return pupds;
    }

    @Override
    protected boolean isCorrectClass(final String className) {
        return PER_USER_POOL_CLASSNAME.equals(className);
    }
}