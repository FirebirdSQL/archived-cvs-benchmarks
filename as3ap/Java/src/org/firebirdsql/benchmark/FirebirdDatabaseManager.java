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

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.firebirdsql.pool.BasicAbstractConnectionPool;
import org.firebirdsql.pool.FBConnectionPoolDataSource;
import org.firebirdsql.pool.SimpleDataSource;

/**
 * Firebird-specific database manager.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class FirebirdDatabaseManager extends BenchmarkDatabaseManager {

    private DataSource dataSource;

    /**
     * Create instance of this class.
     * 
     * @throws SQLException
     */
    public FirebirdDatabaseManager() throws SQLException {
        switch (getConfig().getPoolingType()) {
            case BenchmarkConfiguration.NO_POOLING:
                dataSource = getNoPoolingDataSource();
                break;

            case BenchmarkConfiguration.CONNECTION_POOLING:
                dataSource = getConnectionPoolingDataSource();
                break;

            case BenchmarkConfiguration.STATEMENT_POOLING:
                dataSource = getStatementPoolingDataSource();
                break;

            default:
                throw new SQLException("Pooling type unknown.");
        }
    }

    private BasicAbstractConnectionPool createJdbcConnectionPool()
            throws SQLException {
        FBConnectionPoolDataSource pool = new FBConnectionPoolDataSource();

        pool.setMaxConnections(getConfig().getMaxConnections());

        pool.setDatabase(getConfig().getCustomProperty("firebird.databasePath"));
        pool.setUserName(getConfig().getUserName());
        pool.setPassword(getConfig().getPassword());
        
        pool.setTpbMapping(getConfig().getCustomProperty("firebird.tpbMapping"));

        pool.setType(getConfig().getCustomProperty("firebird.type", "PURE_JAVA"));
        
        return pool;
    }

    private BasicAbstractConnectionPool createPool() throws SQLException {
        return createJdbcConnectionPool();
    }

    private DataSource getNoPoolingDataSource() throws SQLException {
        BasicAbstractConnectionPool pool = createPool();

        pool.setPooling(false);
        pool.setStatementPooling(false);

        return new SimpleDataSource(pool);
    }

    private DataSource getConnectionPoolingDataSource() throws SQLException {
        BasicAbstractConnectionPool pool = createPool();

        pool.setPooling(true);
        pool.setStatementPooling(false);

        return new SimpleDataSource(pool);
    }

    private DataSource getStatementPoolingDataSource() throws SQLException {
        BasicAbstractConnectionPool pool = createPool();

        pool.setPooling(true);
        pool.setStatementPooling(true);

        return new SimpleDataSource(pool);
    }

    public Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        setUp(connection);
        return connection;
    }

}