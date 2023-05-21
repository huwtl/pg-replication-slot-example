package org.huwtl.pgrepl.application.services.replication;

import java.sql.SQLException;

public interface ReplicationStreamProvider {
    ReplicationStream openedReplicationStream() throws SQLException;
}
