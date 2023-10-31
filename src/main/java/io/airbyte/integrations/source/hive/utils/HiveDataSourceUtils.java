package io.airbyte.integrations.source.hive.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.CharSource;
import com.zaxxer.hikari.HikariDataSource;
import io.airbyte.db.jdbc.JdbcUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static io.airbyte.integrations.source.hive.HiveSource.*;

public class HiveDataSourceUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveDataSourceUtils.class);

    public static HikariDataSource createDataSource(final JsonNode config, final JsonNode jdbcConfig) {
        String jdbcUrl = jdbcConfig.get(JdbcUtils.JDBC_URL_KEY).asText();

        LOGGER.info("#### DATALABS HIVE: Config HikariDataSource");

        final HikariDataSource dataSource = new HikariDataSource();
        dataSource.setMaximumPoolSize(10);
        dataSource.setMinimumIdle(0);
        dataSource.setConnectionTimeout(Duration.ofSeconds(90).toMillis());
        dataSource.setInitializationFailTimeout(Integer.MIN_VALUE);

        LOGGER.info("#### DATALABS HIVE: Final JDBC URL {}####", jdbcUrl);

        if(config.has(HIVE_DRIVER_JDBC_CLASS)) {
            dataSource.setDriverClassName(config.get(HIVE_DRIVER_JDBC_CLASS).asText());
        } else {
            dataSource.setDriverClassName(DRIVER_CLASS);
        }

        if(config.has(SSL_TRUST_STORE)) {
            LOGGER.debug("#### DATALABS HIVE: TrustStore present. Create temporary path");

            try {
                java.nio.file.Path kb5tmpFile = Files.createTempFile("trustStore", ".jks");
                Files.write(kb5tmpFile, Base64.getDecoder().decode(config.get(SSL_TRUST_STORE).asText()));

                jdbcUrl = new StringBuilder(jdbcUrl).append(";sslTrustStore=").append(kb5tmpFile).toString();
            } catch(IOException e) {
                LOGGER.error("Cannot load truststore file. Can't create file on temporary directory {}", e.getMessage());
                throw new RuntimeException("Cannot load truststore file. Can't create file on temporary directory", e);
            }
        }

        dataSource.setJdbcUrl(jdbcUrl);

        Configuration configuration = new Configuration();
        try {
            configuration.addResource(CharSource.wrap(config.get(CORE_SITE_CONF).asText()).asByteSource(StandardCharsets.UTF_8).openStream());
            configuration.addResource(CharSource.wrap(config.get(HIVE_SITE_CONF).asText()).asByteSource(StandardCharsets.UTF_8).openStream());

            String kerberosConfig = config.has(KERBEROS_CONF) ? config.get(KERBEROS_CONF).asText() : null;
            String kerberosKeytab = config.has(KEYTAB) ? config.get(KEYTAB).asText() : null;
            String kerberosUser = config.has(KERBEROS_USER) ? config.get(KERBEROS_USER).asText() : null;

            LOGGER.info("#### DATALABS HIVE: Kerberos Config {} {} {}####", kerberosConfig, kerberosKeytab, kerberosUser);

            if(StringUtils.isNotEmpty(kerberosConfig) && StringUtils.isNotEmpty(kerberosKeytab) && StringUtils.isNotEmpty(kerberosUser)) {
                java.nio.file.Path krb5Path = java.nio.file.Path.of("/etc/krb5.conf");
                Files.deleteIfExists(krb5Path);
                Files.createFile(krb5Path);
                Files.write(krb5Path, kerberosConfig.getBytes(StandardCharsets.UTF_8));

                java.nio.file.Path keytabFilePath = java.nio.file.Path.of("/etc/datalabs.keytab");
                Files.deleteIfExists(keytabFilePath);
                Files.createFile(keytabFilePath);
                Files.write(keytabFilePath, Base64.getDecoder().decode(kerberosKeytab));

                execute(new String[]{
                        "/bin/sh",
                        "-c",
                        String.format("kinit -k -t %s %s", keytabFilePath, kerberosUser)
                });

                UserGroupInformation.loginUserFromKeytab(kerberosUser, keytabFilePath.toString());
            }

            UserGroupInformation.setConfiguration(configuration);
        } catch (Exception e) {
            LOGGER.error("Error ", e);
            throw new RuntimeException(e);
        }

        return dataSource;
    }

    /**
     * Execute bash commands on current machine
     * @param command - the multiline command to execute
     * @return Commands stdout
     * @throws Exception In case of commands error
     */
    public static List<String> execute(String[] command) throws Exception {
        List<String> cmdResult = new ArrayList<>();

        Process powerShellProcess = Runtime.getRuntime().exec(command);
        powerShellProcess.getOutputStream().close();

        String line;

        boolean stdOutEmpty = true;
        try (BufferedReader stdout = new BufferedReader(new InputStreamReader(
                powerShellProcess.getInputStream(), StandardCharsets.UTF_8))) {

            while ((line = stdout.readLine()) != null) {
                if (stdOutEmpty) {
                    stdOutEmpty = false;
                }

                LOGGER.info(line);
                cmdResult.add(line);
            }
        } catch (Exception e) {
            LOGGER.error("#### READ: Problem during retrieval of command stdout", e);
        }
        System.out.println();
        boolean stdErrEmpty = true;
        try (BufferedReader stderr = new BufferedReader(new InputStreamReader(
                powerShellProcess.getErrorStream(), StandardCharsets.UTF_8))) {
            while ((line = stderr.readLine()) != null) {
                if (stdErrEmpty) {
                    stdErrEmpty = false;
                }

                LOGGER.error(line);
            }
        }catch (Exception e) {
            LOGGER.error("#### READ: Problem during retrieval of command stderr", e);
        }

        if(stdOutEmpty && !stdErrEmpty) {
            throw new Exception(String.format("#### READ: Problem during bash command execution %s ####", Arrays.toString(command)));
        }

        return cmdResult;
    }

}
