/*
 * Firebird Open Source AS3AP Benchmark suite
 *
 * Distributable under LGPL license.
 * You may obtain a copy of the License at http://www.gnu.org/copyleft/lgpl.html
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * LGPL License for more details.
 *
 * This file was created by members of the firebird development team.
 * All individual contributions remain the Copyright (C) of those
 * individuals.  Contributors to this file are either listed here or
 * can be obtained from a CVS history command.
 *
 * All rights reserved.
 */

package org.firebirdsql.benchmark;

import java.sql.*;

/**
 * This class is responsible for database management.
 * 
 */
public abstract class BenchmarkDatabaseManager {
    
    /**
     * Create instance of this class. 
     * 
     * @param database name of the database, e.g. 
     * <code>"localhost/3050:/tmp/benchmark/as3ap.gdb"</code>. Note, this is 
     * not JDBC URL.
     * 
     * @throws SQLException if database cannot be created.
     */
    public BenchmarkDatabaseManager() throws SQLException { 
    }
    
    public BenchmarkConfiguration getConfig() {
        return BenchmarkConfiguration.getConfiguration();
    }

    /**
     * Get new connection. 
     * 
     * @return insance of {@link Connection}
     * 
     * @throws SQLException if connection cannot be obtained.
     */
    public abstract Connection getConnection() throws SQLException;
    
    /**
     * Release JDBC connection that was obtained using {@link #getConnection()}
     * method.
     * 
     * @param connection connection to release.
     * 
     * @throws SQLException if connection could not be released.
     */    
    public void releaseConnection(Connection connection) throws SQLException {
        tearDown(connection);
        connection.close();
    }
    
    /**
     * Set up JDBC connection before giving it to the requester. This method 
     * should be overrided in order to model various scenarios. 
     * 
     * @param connection JDBC connection to set up.
     * 
     * @throws SQLException if something happened during connection set up.
     */
    protected void setUp(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        
        int isolation = getConfig().getTransactionIsolation();
        
        if (isolation != -1)
            connection.setTransactionIsolation(isolation);
    }
    
    /**
     * Tear down connection before releasing it.
     * 
     * @param connection connection to tear down.
     * 
     * @throws SQLException if something happened during connection tear down.
     */
    protected void tearDown(Connection connection) throws SQLException {
        // commit transaction if we are running in non-autocommit mode
        if (!connection.getAutoCommit())
            connection.commit();
    }
    
    /**
     * Execute update statement in a separate transaction.
     * 
     * @param sql SQL statememnt to execute.
     * 
     * @throws SQLException if statement could not be executed.
     */
    public void executeDDL(String sql) throws SQLException {
        Connection connection = getConnection();

        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(sql);
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } finally {
                releaseConnection(connection);
            }
        }
    }
}