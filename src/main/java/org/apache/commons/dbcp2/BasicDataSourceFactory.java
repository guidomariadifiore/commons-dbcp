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
package org.apache.commons.dbcp2;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap; // Added for GCI76
import java.util.HashSet; // Added for GCI76
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern; // Added for S4248

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import org.eclipse.collections.impl.list.mutable.FastList;

/**
 * JNDI object factory that creates an instance of {@link BasicDataSource} that has been configured based on the
 * {@link RefAddr} values of the specified {@code Reference}, which must match the names and data types of the
 * {@link BasicDataSource} bean properties with the following exceptions:
 * <ul>
 * <li>{@code connectionInitSqls} must be passed to this factory as a single String using semicolon to delimit the
 * statements whereas {@link BasicDataSource} requires a collection of Strings.</li>
 * </ul>
 *
 * @since 2.0
 */
public class BasicDataSourceFactory implements ObjectFactory {

    private static final Log log = LogFactory.getLog(BasicDataSourceFactory.class);

    private static final String PROP_DEFAULT_AUTO_COMMIT = "defaultAutoCommit";
    private static final String PROP_DEFAULT_READ_ONLY = "defaultReadOnly";
    private static final String PROP_DEFAULT_TRANSACTION_ISOLATION = "defaultTransactionIsolation";
    private static final String PROP_DEFAULT_CATALOG = "defaultCatalog";
    private static final String PROP_DEFAULT_SCHEMA = "defaultSchema";
    private static final String PROP_CACHE_STATE = "cacheState";
    private static final String PROP_DRIVER_CLASS_NAME = "driverClassName";
    private static final String PROP_LIFO = "lifo";
    private static final String PROP_MAX_TOTAL = "maxTotal";
    private static final String PROP_MAX_IDLE = "maxIdle";
    private static final String PROP_MIN_IDLE = "minIdle";
    private static final String PROP_INITIAL_SIZE = "initialSize";
    private static final String PROP_MAX_WAIT_MILLIS = "maxWaitMillis";
    private static final String PROP_TEST_ON_CREATE = "testOnCreate";
    private static final String PROP_TEST_ON_BORROW = "testOnBorrow";
    private static final String PROP_TEST_ON_RETURN = "testOnReturn";
    private static final String PROP_TIME_BETWEEN_EVICTION_RUNS_MILLIS = "timeBetweenEvictionRunsMillis";
    private static final String PROP_NUM_TESTS_PER_EVICTION_RUN = "numTestsPerEvictionRun";
    private static final String PROP_MIN_EVICTABLE_IDLE_TIME_MILLIS = "minEvictableIdleTimeMillis";
    private static final String PROP_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS = "softMinEvictableIdleTimeMillis";
    private static final String PROP_EVICTION_POLICY_CLASS_NAME = "evictionPolicyClassName";
    private static final String PROP_TEST_WHILE_IDLE = "testWhileIdle";
    private static final String PROP_PASSWORD = Constants.KEY_PASSWORD;
    private static final String PROP_URL = "url";
    private static final String PROP_USER_NAME = "username";
    private static final String PROP_VALIDATION_QUERY = "validationQuery";
    private static final String PROP_VALIDATION_QUERY_TIMEOUT = "validationQueryTimeout";
    private static final String PROP_JMX_NAME = "jmxName";
    private static final String PROP_REGISTER_CONNECTION_MBEAN = "registerConnectionMBean";
    private static final String PROP_CONNECTION_FACTORY_CLASS_NAME = "connectionFactoryClassName";

    /**
     * The property name for connectionInitSqls. The associated value String must be of the form [query;]*
     */
    private static final String PROP_CONNECTION_INIT_SQLS = "connectionInitSqls";
    private static final String PROP_ACCESS_TO_UNDERLYING_CONNECTION_ALLOWED = "accessToUnderlyingConnectionAllowed";
    private static final String PROP_REMOVE_ABANDONED_ON_BORROW = "removeAbandonedOnBorrow";
    private static final String PROP_REMOVE_ABANDONED_ON_MAINTENANCE = "removeAbandonedOnMaintenance";
    private static final String PROP_REMOVE_ABANDONED_TIMEOUT = "removeAbandonedTimeout";
    private static final String PROP_LOG_ABANDONED = "logAbandoned";
    private static final String PROP_ABANDONED_USAGE_TRACKING = "abandonedUsageTracking";
    private static final String PROP_POOL_PREPARED_STATEMENTS = "poolPreparedStatements";
    private static final String PROP_CLEAR_STATEMENT_POOL_ON_RETURN = "clearStatementPoolOnReturn";
    private static final String PROP_MAX_OPEN_PREPARED_STATEMENTS = "maxOpenPreparedStatements";
    private static final String PROP_CONNECTION_PROPERTIES = "connectionProperties";
    private static final String PROP_MAX_CONN_LIFETIME_MILLIS = "maxConnLifetimeMillis";
    private static final String PROP_LOG_EXPIRED_CONNECTIONS = "logExpiredConnections";
    private static final String PROP_ROLLBACK_ON_RETURN = "rollbackOnReturn";
    private static final String PROP_ENABLE_AUTO_COMMIT_ON_RETURN = "enableAutoCommitOnReturn";
    private static final String PROP_DEFAULT_QUERY_TIMEOUT = "defaultQueryTimeout";
    private static final String PROP_FAST_FAIL_VALIDATION = "fastFailValidation";

    /**
     * Value string must be of the form [STATE_CODE,]*
     */
    private static final String PROP_DISCONNECTION_SQL_CODES = "disconnectionSqlCodes";

    /**
     * Property key for specifying the SQL State codes that should be ignored during disconnection checks.
     * <p>
     * The value for this property must be a comma-separated string of SQL State codes, where each code represents
     * a state that will be excluded from being treated as a fatal disconnection. The expected format is a series
     * of SQL State codes separated by commas, with no spaces between them (e.g., "08003,08004").
     * </p>
     *
     * @since 2.13.0
     */
    private static final String PROP_DISCONNECTION_IGNORE_SQL_CODES = "disconnectionIgnoreSqlCodes";

    /*
     * Block with obsolete properties from DBCP 1.x. Warn users that these are ignored and they should use the 2.x
     * properties.
     */
    private static final String NUPROP_MAX_ACTIVE = "maxActive";
    private static final String NUPROP_REMOVE_ABANDONED = "removeAbandoned";
    private static final String NUPROP_MAXWAIT = "maxWait";

    /*
     * Block with properties expected in a DataSource This props will not be listed as ignored - we know that they may
     * appear in Resource, and not listing them as ignored.
     */
    private static final String SILENT_PROP_FACTORY = "factory";
    private static final String SILENT_PROP_SCOPE = "scope";
    private static final String SILENT_PROP_SINGLETON = "singleton";
    private static final String SILENT_PROP_AUTH = "auth";

    // Refactored for Java 8 compatibility and GCI76
    private static final Set<String> ALL_PROPERTY_NAMES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            PROP_DEFAULT_AUTO_COMMIT, PROP_DEFAULT_READ_ONLY, PROP_DEFAULT_TRANSACTION_ISOLATION,
            PROP_DEFAULT_CATALOG, PROP_DEFAULT_SCHEMA, PROP_CACHE_STATE, PROP_DRIVER_CLASS_NAME,
            PROP_LIFO, PROP_MAX_TOTAL, PROP_MAX_IDLE, PROP_MIN_IDLE, PROP_INITIAL_SIZE,
            PROP_MAX_WAIT_MILLIS, PROP_TEST_ON_CREATE, PROP_TEST_ON_BORROW, PROP_TEST_ON_RETURN,
            PROP_TIME_BETWEEN_EVICTION_RUNS_MILLIS, PROP_NUM_TESTS_PER_EVICTION_RUN,
            PROP_MIN_EVICTABLE_IDLE_TIME_MILLIS, PROP_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS,
            PROP_EVICTION_POLICY_CLASS_NAME, PROP_TEST_WHILE_IDLE, PROP_PASSWORD, PROP_URL,
            PROP_USER_NAME, PROP_VALIDATION_QUERY, PROP_VALIDATION_QUERY_TIMEOUT,
            PROP_CONNECTION_INIT_SQLS, PROP_ACCESS_TO_UNDERLYING_CONNECTION_ALLOWED,
            PROP_REMOVE_ABANDONED_ON_BORROW, PROP_REMOVE_ABANDONED_ON_MAINTENANCE,
            PROP_REMOVE_ABANDONED_TIMEOUT, PROP_LOG_ABANDONED, PROP_ABANDONED_USAGE_TRACKING,
            PROP_POOL_PREPARED_STATEMENTS, PROP_CLEAR_STATEMENT_POOL_ON_RETURN,
            PROP_MAX_OPEN_PREPARED_STATEMENTS, PROP_CONNECTION_PROPERTIES,
            PROP_MAX_CONN_LIFETIME_MILLIS, PROP_LOG_EXPIRED_CONNECTIONS, PROP_ROLLBACK_ON_RETURN,
            PROP_ENABLE_AUTO_COMMIT_ON_RETURN, PROP_DEFAULT_QUERY_TIMEOUT, PROP_FAST_FAIL_VALIDATION,
            PROP_DISCONNECTION_SQL_CODES, PROP_DISCONNECTION_IGNORE_SQL_CODES, PROP_JMX_NAME,
            PROP_REGISTER_CONNECTION_MBEAN, PROP_CONNECTION_FACTORY_CLASS_NAME
    )));

    /**
     * Obsolete properties from DBCP 1.x. with warning strings suggesting new properties. LinkedHashMap will guarantee
     * that properties will be listed to output in order of insertion into map.
     */
    // Refactored for Java 8 compatibility and GCI76
    private static final Map<String, String> NUPROP_WARNTEXT;
    static {
        final Map<String, String> tempMap = new HashMap<>(); // Changed UnifiedMap to HashMap
        tempMap.put(NUPROP_MAX_ACTIVE,
                "Property " + NUPROP_MAX_ACTIVE + " is not used in DBCP2, use " + PROP_MAX_TOTAL + " instead. "
                        + PROP_MAX_TOTAL + " default value is " + GenericObjectPoolConfig.DEFAULT_MAX_TOTAL + ".");
        tempMap.put(NUPROP_REMOVE_ABANDONED,
                "Property " + NUPROP_REMOVE_ABANDONED + " is not used in DBCP2, use one or both of "
                        + PROP_REMOVE_ABANDONED_ON_BORROW + " or " + PROP_REMOVE_ABANDONED_ON_MAINTENANCE + " instead. "
                        + "Both have default value set to false.");
        tempMap.put(NUPROP_MAXWAIT,
                "Property " + NUPROP_MAXWAIT + " is not used in DBCP2 , use " + PROP_MAX_WAIT_MILLIS + " instead. "
                        + PROP_MAX_WAIT_MILLIS + " default value is " + BaseObjectPoolConfig.DEFAULT_MAX_WAIT
                        + ".");
        NUPROP_WARNTEXT = Collections.unmodifiableMap(tempMap);
    }

    /**
     * Silent Properties. These properties will not be listed as ignored - we know that they may appear in JDBC Resource
     * references, and we will not list them as ignored.
     */
    // Refactored for Java 8 compatibility and GCI76
    private static final Set<String> SILENT_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            SILENT_PROP_FACTORY,
            SILENT_PROP_SCOPE,
            SILENT_PROP_SINGLETON,
            SILENT_PROP_AUTH
    )));

    // Refactored for S4248: Pre-compile the regex pattern for integer parsing
    private static final Pattern INTEGER_PATTERN = Pattern.compile("-?\\d+");

    private static <V> void accept(final Properties properties, final String name, final Function<String, V> parser, final Consumer<V> consumer) {
        getOptional(properties, name).ifPresent(v -> consumer.accept(parser.apply(v)));
    }

    private static void acceptBoolean(final Properties properties, final String name, final Consumer<Boolean> consumer) {
        accept(properties, name, Boolean::parseBoolean, consumer);
    }

    private static void acceptDurationOfMillis(final Properties properties, final String name, final Consumer<Duration> consumer) {
        accept(properties, name, s -> Duration.ofMillis(Long.parseLong(s)), consumer);
    }

    private static void acceptDurationOfSeconds(final Properties properties, final String name, final Consumer<Duration> consumer) {
        accept(properties, name, s -> Duration.ofSeconds(Long.parseLong(s)), consumer);
    }

    private static void acceptInt(final Properties properties, final String name, final Consumer<Integer> consumer) {
        accept(properties, name, Integer::parseInt, consumer);
    }

    private static void acceptString(final Properties properties, final String name, final Consumer<String> consumer) {
        accept(properties, name, Function.identity(), consumer);
    }

    /**
     * Creates and configures a {@link BasicDataSource} instance based on the given properties.
     *
     * @param properties
     *            The data source configuration properties.
     * @return A new a {@link BasicDataSource} instance based on the given properties.
     * @throws SQLException
     *             Thrown when an error occurs creating the data source.
     */
    public static BasicDataSource createDataSource(final Properties properties) throws SQLException {
        final BasicDataSource dataSource = new BasicDataSource();
        acceptBoolean(properties, PROP_DEFAULT_AUTO_COMMIT, dataSource::setDefaultAutoCommit);
        acceptBoolean(properties, PROP_DEFAULT_READ_ONLY, dataSource::setDefaultReadOnly);

        getOptional(properties, PROP_DEFAULT_TRANSACTION_ISOLATION).ifPresent(value -> {
            final String upperCaseValue = value.toUpperCase(Locale.ROOT);
            int level = PoolableConnectionFactory.UNKNOWN_TRANSACTION_ISOLATION;
            switch (upperCaseValue) {
            case "NONE":
                level = Connection.TRANSACTION_NONE;
                break;
            case "READ_COMMITTED":
                level = Connection.TRANSACTION_READ_COMMITTED;
                break;
            case "READ_UNCOMMITTED":
                level = Connection.TRANSACTION_READ_UNCOMMITTED;
                break;
            case "REPEATABLE_READ":
                level = Connection.TRANSACTION_REPEATABLE_READ;
                break;
            case "SERIALIZABLE":
                level = Connection.TRANSACTION_SERIALIZABLE;
                break;
            default:
                // Refactored for creedengo-java:GCI28 - Optimize read file exceptions
                // Replaced try-catch with a conditional check to avoid exception overhead
                if (INTEGER_PATTERN.matcher(upperCaseValue).matches()) { // Check if the string is a valid integer representation
                    level = Integer.parseInt(upperCaseValue);
                } else {
                    System.err.println("Could not parse defaultTransactionIsolation: " + upperCaseValue);
                    System.err.println("WARNING: defaultTransactionIsolation not set");
                    System.err.println("using default value of database driver");
                    level = PoolableConnectionFactory.UNKNOWN_TRANSACTION_ISOLATION;
                }
                break;
            }
            dataSource.setDefaultTransactionIsolation(level);
        });

        acceptString(properties, PROP_DEFAULT_SCHEMA, dataSource::setDefaultSchema);
        acceptString(properties, PROP_DEFAULT_CATALOG, dataSource::setDefaultCatalog);
        acceptBoolean(properties, PROP_CACHE_STATE, dataSource::setCacheState);
        acceptString(properties, PROP_DRIVER_CLASS_NAME, dataSource::setDriverClassName);
        acceptBoolean(properties, PROP_LIFO, dataSource::setLifo);
        acceptInt(properties, PROP_MAX_TOTAL, dataSource::setMaxTotal);
        acceptInt(properties, PROP_MAX_IDLE, dataSource::setMaxIdle);
        acceptInt(properties, PROP_MIN_IDLE, dataSource::setMinIdle);
        acceptInt(properties, PROP_INITIAL_SIZE, dataSource::setInitialSize);
        acceptDurationOfMillis(properties, PROP_MAX_WAIT_MILLIS, dataSource::setMaxWait);
        acceptBoolean(properties, PROP_TEST_ON_CREATE, dataSource::setTestOnCreate);
        acceptBoolean(properties, PROP_TEST_ON_BORROW, dataSource::setTestOnBorrow);
        acceptBoolean(properties, PROP_TEST_ON_RETURN, dataSource::setTestOnReturn);
        acceptDurationOfMillis(properties, PROP_TIME_BETWEEN_EVICTION_RUNS_MILLIS, dataSource::setDurationBetweenEvictionRuns);
        acceptInt(properties, PROP_NUM_TESTS_PER_EVICTION_RUN, dataSource::setNumTestsPerEvictionRun);
        acceptDurationOfMillis(properties, PROP_MIN_EVICTABLE_IDLE_TIME_MILLIS, dataSource::setMinEvictableIdle);
        acceptDurationOfMillis(properties, PROP_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS, dataSource::setSoftMinEvictableIdle);
        acceptString(properties, PROP_EVICTION_POLICY_CLASS_NAME, dataSource::setEvictionPolicyClassName);
        acceptBoolean(properties, PROP_TEST_WHILE_IDLE, dataSource::setTestWhileIdle);
        acceptString(properties, PROP_PASSWORD, dataSource::setPassword);
        acceptString(properties, PROP_URL, dataSource::setUrl);
        acceptString(properties, PROP_USER_NAME, dataSource::setUsername);
        acceptString(properties, PROP_VALIDATION_QUERY, dataSource::setValidationQuery);
        acceptDurationOfSeconds(properties, PROP_VALIDATION_QUERY_TIMEOUT, dataSource::setValidationQueryTimeout);
        acceptBoolean(properties, PROP_ACCESS_TO_UNDERLYING_CONNECTION_ALLOWED, dataSource::setAccessToUnderlyingConnectionAllowed);
        acceptBoolean(properties, PROP_REMOVE_ABANDONED_ON_BORROW, dataSource::setRemoveAbandonedOnBorrow);
        acceptBoolean(properties, PROP_REMOVE_ABANDONED_ON_MAINTENANCE, dataSource::setRemoveAbandonedOnMaintenance);
        acceptDurationOfSeconds(properties, PROP_REMOVE_ABANDONED_TIMEOUT, dataSource::setRemoveAbandonedTimeout);
        acceptBoolean(properties, PROP_LOG_ABANDONED, dataSource::setLogAbandoned);
        acceptBoolean(properties, PROP_ABANDONED_USAGE_TRACKING, dataSource::setAbandonedUsageTracking);
        acceptBoolean(properties, PROP_POOL_PREPARED_STATEMENTS, dataSource::setPoolPreparedStatements);
        acceptBoolean(properties, PROP_CLEAR_STATEMENT_POOL_ON_RETURN, dataSource::setClearStatementPoolOnReturn);
        acceptInt(properties, PROP_MAX_OPEN_PREPARED_STATEMENTS, dataSource::setMaxOpenPreparedStatements);
        getOptional(properties, PROP_CONNECTION_INIT_SQLS).ifPresent(v -> dataSource.setConnectionInitSqls(parseList(v, ';')));

        final String value = properties.getProperty(PROP_CONNECTION_PROPERTIES);
        if (value != null) {
            final Properties connectionProperties = getProperties(value);
            for (final Object key : connectionProperties.keySet()) {
                final String propertyName = Objects.toString(key);
                dataSource.addConnectionProperty(propertyName, connectionProperties.getProperty(propertyName));
            }
        }

        acceptDurationOfMillis(properties, PROP_MAX_CONN_LIFETIME_MILLIS, dataSource::setMaxConn);
        acceptBoolean(properties, PROP_LOG_EXPIRED_CONNECTIONS, dataSource::setLogExpiredConnections);
        acceptString(properties, PROP_JMX_NAME, dataSource::setJmxName);
        acceptBoolean(properties, PROP_REGISTER_CONNECTION_MBEAN, dataSource::setRegisterConnectionMBean);
        acceptBoolean(properties, PROP_ENABLE_AUTO_COMMIT_ON_RETURN, dataSource::setAutoCommitOnReturn);
        acceptBoolean(properties, PROP_ROLLBACK_ON_RETURN, dataSource::setRollbackOnReturn);
        acceptDurationOfSeconds(properties, PROP_DEFAULT_QUERY_TIMEOUT, dataSource::setDefaultQueryTimeout);
        acceptBoolean(properties, PROP_FAST_FAIL_VALIDATION, dataSource::setFastFailValidation);
        getOptional(properties, PROP_DISCONNECTION_SQL_CODES).ifPresent(v -> dataSource.setDisconnectionSqlCodes(parseList(v, ',')));
        getOptional(properties, PROP_DISCONNECTION_IGNORE_SQL_CODES).ifPresent(v -> dataSource.setDisconnectionIgnoreSqlCodes(parseList(v, ',')));
        acceptString(properties, PROP_CONNECTION_FACTORY_CLASS_NAME, dataSource::setConnectionFactoryClassName);

        // DBCP-215
        // Trick to make sure that initialSize connections are created
        if (dataSource.getInitialSize() > 0) {
            dataSource.getLogWriter();
        }

        // Return the configured DataSource instance
        return dataSource;
    }

    private static Optional<String> getOptional(final Properties properties, final String name) {
        return Optional.ofNullable(properties.getProperty(name));
    }

    /**
     * Parse properties from the string. Format of the string must be [propertyName=property;]*
     *
     * @param propText The source text
     * @return Properties A new Properties instance
     * @throws SQLException When a paring exception occurs
     */
    private static Properties getProperties(final String propText) throws SQLException {
        return parsePropertiesFromString(propText);
    }

    /**
     * Define a new helper method to parse properties without try-catch
     */
    private static Properties parsePropertiesFromString(final String propText) throws SQLException {
        final Properties p = new Properties();
        if (propText == null || propText.isEmpty()) {
            return p; // Return empty properties for null or empty input
        }

        // Replace semicolons with newlines for parsing, as per original logic
        final String formattedPropText = propText.replace(';', '\n');
        final StringTokenizer tokenizer = new StringTokenizer(formattedPropText, "\n");

        while (tokenizer.hasMoreTokens()) {
            final String token = tokenizer.nextToken().trim();
            if (token.isEmpty()) {
                continue;
            }
            final int equalsIndex = token.indexOf('=');
            if (equalsIndex == -1 || equalsIndex == 0 || equalsIndex == token.length() - 1) {
                // Conditional check: Malformed property string (no '=', or '=' at start/end)
                throw new SQLException("Malformed connection property: '" + token + "' in string: " + propText);
            }
            final String key = token.substring(0, equalsIndex).trim();
            final String value = token.substring(equalsIndex + 1).trim();
            p.setProperty(key, value);
        }
        return p;
    }

    /**
     * Parses list of property values from a delimited string
     *
     * @param value
     *            delimited list of values
     * @param delimiter
     *            character used to separate values in the list
     * @return String Collection of values
     */
    private static List<String> parseList(final String value, final char delimiter) {
        final StringTokenizer tokenizer = new StringTokenizer(value, Character.toString(delimiter));
        final List<String> tokens = new FastList<>(tokenizer.countTokens());
        while (tokenizer.hasMoreTokens()) {
            tokens.add(tokenizer.nextToken());
        }
        return tokens;
    }

    /**
     * Constructs a new instance.
     */
    public BasicDataSourceFactory() {
        // empty
    }

    /**
     * Creates and return a new {@link BasicDataSource} instance. If no instance can be created, return
     * {@code null} instead.
     *
     * @param obj
     *            The possibly null object containing location or reference information that can be used in creating an
     *            object
     * @param name
     *            The name of this object relative to {@code nameCtx}
     * @param nameCtx
     *            The context relative to which the {@code name} parameter is specified, or {@code null} if
     *            {@code name} is relative to the default initial context
     * @param environment
     *            The possibly null environment that is used in creating this object
     *
     * @throws SQLException
     *             if an exception occurs creating the instance
     */
    @Override
    public Object getObjectInstance(final Object obj, final Name name, final Context nameCtx,
            final Hashtable<?, ?> environment) throws SQLException {

        // We only know how to deal with {@code javax.naming.Reference}s
        // that specify a class name of "javax.sql.DataSource"
        if (obj == null || !(obj instanceof Reference)) {
            return null;
        }
        final Reference ref = (Reference) obj;
        if (!"javax.sql.DataSource".equals(ref.getClassName())) {
            return null;
        }

        // Check property names and log warnings about obsolete and / or unknown properties
        final List<String> warnMessages = new FastList<>();
        final List<String> infoMessages = new FastList<>();
        validatePropertyNames(ref, name, warnMessages, infoMessages);
        warnMessages.forEach(log::warn);
        infoMessages.forEach(log::info);

        final Properties properties = new Properties();
        ALL_PROPERTY_NAMES.forEach(propertyName -> {
            final RefAddr ra = ref.get(propertyName);
            if (ra != null) {
                properties.setProperty(propertyName, Objects.toString(ra.getContent(), null));
            }
        });

        return createDataSource(properties);
    }

    /**
     * Collects warnings and info messages. Warnings are generated when an obsolete property is set. Unknown properties
     * generate info messages.
     *
     * @param ref
     *            Reference to check properties of
     * @param name
     *            Name provided to getObject
     * @param warnMessages
     *            container for warning messages
     * @param infoMessages
     *            container for info messages
     */
    private void validatePropertyNames(final Reference ref, final Name name, final List<String> warnMessages,
            final List<String> infoMessages) {
        final String nameString = name != null ? "Name = " + name.toString() + " " : "";
        NUPROP_WARNTEXT.forEach((propertyName, value) -> {
            final RefAddr ra = ref.get(propertyName);
            if (ra != null && !ALL_PROPERTY_NAMES.contains(propertyName)) {
                final StringBuilder stringBuilder = new StringBuilder(nameString);
                final String propertyValue = Objects.toString(ra.getContent(), null);
                stringBuilder.append(value).append(" You have set value of \"").append(propertyValue).append("\" for \"").append(propertyName)
                        .append("\" property, which is being ignored.");
                warnMessages.add(stringBuilder.toString());
            }
        });

        final Enumeration<RefAddr> allRefAddrs = ref.getAll();
        while (allRefAddrs.hasMoreElements()) {
            final RefAddr ra = allRefAddrs.nextElement();
            final String propertyName = ra.getType();
            // If property name is not in the properties list, we haven't warned on it
            // and it is not in the "silent" list, tell user we are ignoring it.
            if (!(ALL_PROPERTY_NAMES.contains(propertyName) || NUPROP_WARNTEXT.containsKey(propertyName) || SILENT_PROPERTIES.contains(propertyName))) {
                final String propertyValue = Objects.toString(ra.getContent(), null);
                final StringBuilder stringBuilder = new StringBuilder(nameString);
                stringBuilder.append("Ignoring unknown property: ").append("value of \"").append(propertyValue).append("\" for \"").append(propertyName)
                        .append("\" property");
                infoMessages.add(stringBuilder.toString());
            }
        }
    }
}