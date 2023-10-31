/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.hive;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.SqlDatabase;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.db.jdbc.StreamingJdbcDatabase;
import io.airbyte.db.jdbc.streaming.AdaptiveStreamingQueryConfig;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.hive.utils.HiveDataSourceUtils;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.integrations.source.jdbc.dto.JdbcPrivilegeDto;
import io.airbyte.integrations.source.relationaldb.TableInfo;
import io.airbyte.protocol.models.CommonField;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.AirbyteCatalog;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteStream;
import io.airbyte.protocol.models.v0.SyncMode;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static io.airbyte.db.jdbc.JdbcConstants.*;
import static io.airbyte.db.jdbc.JdbcConstants.JDBC_DECIMAL_DIGITS;
import static java.util.stream.Collectors.toList;

public class HiveSource extends AbstractJdbcSource<JDBCType> implements Source {

    public static final String CORE_SITE_CONF = "core_site_conf";
    public static final String HIVE_SITE_CONF = "hive_site_conf";
    public static final String HIVE_DRIVER_JDBC_CLASS = "driver_jdbc";

    public static final String KERBEROS_USER = "kerberos_user";
    public static final String KERBEROS_CONF = "kerberos_config";
    public static final String KEYTAB = "keytab";
    public static final String SSL_TRUST_STORE = "ssl_trust_store";

    public static final String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSource.class);

    public HiveSource() {
        super(DRIVER_CLASS, AdaptiveStreamingQueryConfig::new, JdbcUtils.getDefaultSourceOperations());
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        return super.check(config);
    }

    @Override
    public JdbcDatabase createDatabase(JsonNode sourceConfig) throws SQLException {
        final JsonNode jdbcConfig = toDatabaseConfig(sourceConfig);
        // Create the data source
        final DataSource dataSource = HiveDataSourceUtils.createDataSource(sourceConfig, jdbcConfig);
        dataSources.add(dataSource);

        final JdbcDatabase database = new StreamingJdbcDatabase(
                dataSource,
                sourceOperations,
                streamingQueryConfigProvider);

        quoteString = (quoteString == null ? database.getMetaData().getIdentifierQuoteString() : quoteString);
        database.setSourceConfig(sourceConfig);
        database.setDatabaseConfig(jdbcConfig);
        return database;
    }

    @Override
    public JsonNode toDatabaseConfig(final JsonNode config) {
        LOGGER.info("#### DATALABS HIVE: Initialize JDBC configurations");
        String hostkey = config.get(JdbcUtils.HOST_KEY).asText();
        String finalHost = String.format("%s:%s", config.get(JdbcUtils.HOST_KEY).asText(), config.get(JdbcUtils.PORT_KEY).asText());
        if (hostkey.split(",").length > 0) {
            finalHost = Arrays.stream(hostkey.split(",")).map(sHost -> String.format("%s:%s", sHost, config.get(JdbcUtils.PORT_KEY).asText())).collect(Collectors.joining(","));
        }

        String jdbcParamsUrl = "";
        if (config.has(JdbcUtils.JDBC_URL_PARAMS_KEY) && config.get(JdbcUtils.JDBC_URL_PARAMS_KEY).asText().trim().length() > 0) {
            jdbcParamsUrl = new StringBuilder(";").append(config.get(JdbcUtils.JDBC_URL_PARAMS_KEY).asText().replace("&", ";")).toString();
        }

        final String jdbcUrl = String.format("jdbc:hive2://%s/%s%s",
                finalHost,
                config.get(JdbcUtils.DATABASE_KEY).asText(),
                jdbcParamsUrl);

        final ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
                .put(JdbcUtils.JDBC_URL_KEY, jdbcUrl)
                .put(JdbcUtils.SCHEMA_KEY, config.get(JdbcUtils.DATABASE_KEY));


        if (config.has(JdbcUtils.JDBC_URL_PARAMS_KEY)) {
            configBuilder.put(JdbcUtils.JDBC_URL_PARAMS_KEY, config.get(JdbcUtils.JDBC_URL_PARAMS_KEY));
        }

        return Jsons.jsonNode(configBuilder.build());
    }

    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {
        final AirbyteCatalog catalog = super.discover(config);

        LOGGER.info("catalog {}", catalog.getStreams().size());

        final List<AirbyteStream> streams = catalog.getStreams().stream()
                .map(HiveSource::overrideSyncModes)
                .map(HiveSource::removeIncrementalWithoutPk)
                .map(HiveSource::setIncrementalToSourceDefined)
                .collect(toList());

        catalog.setStreams(streams);

        return catalog;
    }

    private static AirbyteStream overrideSyncModes(final AirbyteStream stream) {
        return stream.withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
    }

    private static AirbyteStream removeIncrementalWithoutPk(final AirbyteStream stream) {
        if (stream.getSourceDefinedPrimaryKey().isEmpty()) {
            stream.getSupportedSyncModes().remove(SyncMode.INCREMENTAL);
        }

        return stream;
    }

    private static AirbyteStream setIncrementalToSourceDefined(final AirbyteStream stream) {

        if (stream.getSupportedSyncModes().contains(SyncMode.INCREMENTAL)) {
            //stream.setSourceDefinedCursor(true);
        }

        return stream;
    }

    @Override
    public List<TableInfo<CommonField<JDBCType>>> discoverInternal(JdbcDatabase database) throws Exception {
        String catalog = getCatalog(database);
        String query = "SELECT c.table_schema as table_schema, c.table_name as table_name, c.column_name as column_name, c.data_type as data_type, c.column_default as column_default, c.is_nullable as is_nullable, c.character_maximum_length as character_maximum_length, " +
                "c.numeric_precision as numeric_precision, c.numeric_precision_radix as numeric_precision_radix, c.datetime_precision as datetime_precision " +
                "FROM information_schema.columns c " +
                "WHERE c.table_schema = '" + catalog + "'" +
                "and c.table_name in ( " +
                        "SELECT t.table_name " +
                        "FROM information_schema.tables t " +
                        "WHERE t.table_schema = '" + catalog + "' and t.table_type = 'BASE_TABLE' " +
                ")";

        final Set<JdbcPrivilegeDto> tablesWithSelectGrantPrivilege = getPrivilegesTableForCurrentUser(database, null);
        final Set<String> internalSchemas = new HashSet<>(getExcludedInternalNameSpaces());

        return database.bufferedResultSetQuery(
                conn -> conn.createStatement().executeQuery(query),
                this::getColumnMetadata
        ).stream()
                .filter(excludeNotAccessibleTables(internalSchemas, tablesWithSelectGrantPrivilege))
                .collect(Collectors.groupingBy(t -> ImmutablePair.of(t.get(INTERNAL_SCHEMA_NAME).asText(), t.get(INTERNAL_TABLE_NAME).asText())))
                .values()
                .stream()
                .map(fields -> TableInfo.<CommonField<JDBCType>>builder()
                        .nameSpace(fields.get(0).get(INTERNAL_SCHEMA_NAME).asText())
                        .name(fields.get(0).get(INTERNAL_TABLE_NAME).asText())
                        .fields(fields.stream()
                                .map(f -> {
                                    final JDBCType datatype = getDatabaseFieldType(f);
                                    final JsonSchemaType jsonType = getAirbyteType(datatype);
                                    LOGGER.info("Table {} column {} (type {}[{}], nullable {}) -> {} -> {}",
                                            fields.get(0).get(INTERNAL_TABLE_NAME).asText(),
                                            f.get(INTERNAL_COLUMN_NAME).asText(),
                                            f.get(INTERNAL_COLUMN_TYPE_NAME).asText(),
                                            f.get(INTERNAL_COLUMN_SIZE).asInt(),
                                            f.get(INTERNAL_IS_NULLABLE).asBoolean(),
                                            jsonType,
                                            datatype.getName());
                                    return new CommonField<JDBCType>(f.get(INTERNAL_COLUMN_NAME).asText(), datatype) {};
                                })
                                .collect(Collectors.toList()))
                        .cursorFields(extractCursorFields(fields))
                        .build())
                .collect(Collectors.toList());
    }

    private List<String> extractCursorFields(final List<JsonNode> fields) {
        return fields.stream()
                .filter(field -> isCursorType(getDatabaseFieldType(field)))
                .map(field -> field.get(INTERNAL_COLUMN_NAME).asText())
                .collect(toList());
    }

    private JDBCType getDatabaseFieldType(JsonNode field) {
        String columnTypeInternal = field.get(INTERNAL_COLUMN_TYPE_NAME).asText();
        String columnType = columnTypeInternal.toUpperCase();

        switch (columnType) {
            case "DOUBLE" -> {
                return JDBCType.DOUBLE;
            }
            case "INT" -> {
                return JDBCType.INTEGER;
            }
            case "TIMESTAMP" -> {
                return JDBCType.TIMESTAMP;
            }
            case "DATE" -> {
                return JDBCType.DATE;
            }
            default -> {
                return JDBCType.VARCHAR;
            }
        }
    }

    @Override
    public boolean isCursorType(JDBCType type) {
        return type != JDBCType.TIMESTAMP && type != JDBCType.DATE;
    }

    private String getCatalog(final SqlDatabase database) {
        return (database.getSourceConfig().has(JdbcUtils.DATABASE_KEY) ? database.getSourceConfig().get(JdbcUtils.DATABASE_KEY).asText() : null);
    }

    private JsonNode getColumnMetadata(final ResultSet resultSet) throws SQLException {
        final var fieldMap = ImmutableMap.<String, Object>builder()
                .put(INTERNAL_SCHEMA_NAME, resultSet.getString("table_schema"))
                .put(INTERNAL_TABLE_NAME, resultSet.getString("table_name"))
                .put(INTERNAL_COLUMN_NAME, resultSet.getString("column_name"))
                .put(INTERNAL_COLUMN_TYPE, resultSet.getString("data_type"))
                .put(INTERNAL_IS_NULLABLE, resultSet.getString("is_nullable"))
                .put(INTERNAL_COLUMN_TYPE_NAME, resultSet.getString("data_type"));

        if (resultSet.getString("data_type").equals("timestamp")) {
            fieldMap.put(INTERNAL_COLUMN_SIZE, resultSet.getString("datetime_precision"));
        } else if (resultSet.getString("numeric_precision") != null) {
            fieldMap.put(INTERNAL_COLUMN_SIZE, resultSet.getString("numeric_precision"));
            fieldMap.put(INTERNAL_DECIMAL_DIGITS, resultSet.getString("numeric_precision_radix"));
        } else {
            fieldMap.put(INTERNAL_COLUMN_SIZE, resultSet.getString("character_maximum_length") != null ? resultSet.getString("character_maximum_length") : "255");

        }

        return Jsons.jsonNode(fieldMap.build());
    }

    @Override
    protected Map<String, List<String>> discoverPrimaryKeys(final JdbcDatabase database,
                                                            final List<TableInfo<CommonField<JDBCType>>> tableInfos) {

        LOGGER.info("#### DATABALS tableInfos {}", tableInfos.size());

        return tableInfos.stream()
                .collect(Collectors.toMap(
                        tableInfo -> JdbcUtils.getFullyQualifiedTableName(tableInfo.getNameSpace(), tableInfo.getName()),
                        tableInfo -> {
                            try {
                                return database.queryStrings(connection -> {
                                    final String sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = ? and table_name = ?";
                                    final PreparedStatement preparedStatement = connection.prepareStatement(sql);
                                    preparedStatement.setString(1, tableInfo.getNameSpace());
                                    preparedStatement.setString(2, tableInfo.getName());
                                    return preparedStatement.executeQuery();
                                }, resultSet -> resultSet.getString("column_name"));
                            } catch (final SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }));
    }

    @Override
    public Set<String> getExcludedInternalNameSpaces() {
        return Set.of("");
    }

    public static void main(final String[] args) throws Exception {
        final Source source = new HiveSource();
        LOGGER.info("starting source: {}", HiveSource.class);
        new IntegrationRunner(source).run(args);
        LOGGER.info("completed source: {}", HiveSource.class);
    }


}
