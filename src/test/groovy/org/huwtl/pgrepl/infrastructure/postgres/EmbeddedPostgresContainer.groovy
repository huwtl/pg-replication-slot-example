package org.huwtl.pgrepl.infrastructure.postgres

import groovy.sql.Sql
import org.huwtl.pgrepl.DatabaseConfiguration
import org.testcontainers.containers.GenericContainer
import org.testcontainers.images.builder.ImageFromDockerfile

import static java.sql.DriverManager.getConnection

class EmbeddedPostgresContainer implements AutoCloseable {
    private static final PORT = 5432
    private static final USER = "postgres"
    private static final PASSWORD = "postgres"
    private static final DATABASE = "pg_replication_test"

    private final GenericContainer container

    EmbeddedPostgresContainer() {
        container = new GenericContainer(
                new ImageFromDockerfile()
                        .withFileFromClasspath("Dockerfile", "postgres_container/Dockerfile")
                        .withFileFromClasspath("postgresql.conf.sample", "postgres_container/postgresql.conf.sample")
        ).tap {
            withExposedPorts(PORT)
            withEnv("POSTGRES_USER", USER)
            withEnv("POSTGRES_PASSWORD", PASSWORD)
            withEnv("POSTGRES_DB", DATABASE)
            start()
            addShutdownHook(it::stop)
        }
    }

    Sql sqlForDefaultDatabaseAndSchema() {
        new Sql(getConnection(jdbcUrlForDefaultSchema(), USER, PASSWORD))
    }

    Sql sqlForDefaultDatabaseAndCustomSchema(String schema) {
        new Sql(getConnection(jdbcUrlForSchema(schema), USER, PASSWORD))
    }

    Sql sqlForCustomDatabaseAndDefaultSchema(String databaseName) {
        def jdbcUrl = "jdbc:postgresql://${host()}:${port()}/$databaseName"
        new Sql(getConnection(jdbcUrl, USER, PASSWORD))
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
