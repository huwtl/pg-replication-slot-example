package org.huwtl.pgrepl.consumer;

import java.sql.SQLException;

public interface ReplicationStreamProvider {
    ReplicationStream openedReplicationStream() throws SQLException;
}
