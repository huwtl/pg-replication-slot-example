package org.huwtl.pgrepl.db

import groovy.sql.Sql
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres

import static org.apache.logging.log4j.LogManager.getLogger

class EmbeddedDatabase implements AutoCloseable {
    private static final LOGGER = getLogger()

    private final EmbeddedPostgres database

    EmbeddedDatabase() {
        database = EmbeddedPostgres.builder().start()
    }

    @Override
    void close() {
        LOGGER.info("shutting down embedded database")
        database.close()
    }

    Sql sql() {
        new Sql(database.postgresDatabase)
    }
}
