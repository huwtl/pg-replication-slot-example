package org.huwtl.pgrepl.db

import groovy.sql.Sql
import org.huwtl.pgrepl.DatabaseConfiguration
import org.testcontainers.containers.GenericContainer

import static java.sql.DriverManager.getConnection

class EmbeddedPostgresContainer implements AutoCloseable {
    private static final CONTAINER_VERSION = "debezium/postgres:11-alpine"
    private static final PORT = 5432

    private static final USER = "postgres"
    private static final PASSWORD = "postgres"
    private static final DATABASE = "pg_replication_test"

    private final GenericContainer container

    EmbeddedPostgresContainer() {
        container = new GenericContainer(CONTAINER_VERSION).tap {
            withExposedPorts(PORT)
            withEnv("POSTGRES_USER", USER)
            withEnv("POSTGRES_PASSWORD", PASSWORD)
            withEnv("POSTGRES_DB", DATABASE)
            start()
            addShutdownHook(it::stop)
        }
    }

    Sql sql() {
        new Sql(getConnection(jdbcUrlForDefaultSchema(), USER, PASSWORD))
    }

    Sql sqlForSchema(String schema) {
        new Sql(getConnection(jdbcUrlForSchema(schema), USER, PASSWORD))
    }

    String host() {
        container.host
    }

    String port() {
        container.firstMappedPort
    }

    DatabaseConfiguration configuration() {
        DatabaseConfiguration.builder()
                .port(port())
                .username(username())
                .password(password())
                .database(database())
                .host(host())
                .build()
    }

    private String jdbcUrlForDefaultSchema() {
        "jdbc:postgresql://${host()}:${port()}/$DATABASE"
    }

    private jdbcUrlForSchema(String schema) {
        "${jdbcUrlForDefaultSchema()}?currentSchema=${schema}"
    }

    @Override
    void close() throws Exception {
        container.close()
    }

    static String username() {
        USER
    }

    static String password() {
        PASSWORD
    }

    static String database() {
        DATABASE
    }
}
