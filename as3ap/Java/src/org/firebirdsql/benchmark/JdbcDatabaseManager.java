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

import javax.sql.ConnectionPoolDataSource;

import org.firebirdsql.pool.DriverConnectionPoolDataSource;
import org.firebirdsql.pool.SimpleDataSource;

/**
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class JdbcDatabaseManager extends BenchmarkDatabaseManager {

    private ConnectionPoolDataSource pool;
    private SimpleDataSource dataSource;
    
    /**
     * @throws SQLException
     */
    public JdbcDatabaseManager() throws SQLException {
        super();
        
        pool = createJdbcConnectionPool();
        dataSource = new SimpleDataSource(pool);
    }

    public Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        setUp(connection);
        return connection;
    }

    /**
     * Create JDBC connection and statement pool.
     * 
     * @return instance of {@link ConnectionPoolDataSource}.
     * 
     * @throws SQLException if something went wrong.
     */
    private ConnectionPoolDataSource createJdbcConnectionPool()
            throws SQLException {
        DriverConnectionPoolDataSource pool = new DriverConnectionPoolDataSource();

        pool.setMaxPoolSize(getConfig().getMaxConnections());

        pool.setDriverClassName(getConfig().getDriverClassName());
        pool.setJdbcUrl(getConfig().getJdbcUrl());
        pool.setProperty("user", getConfig().getUserName());
        pool.setProperty("password", getConfig().getPassword());

        switch (getConfig().getPoolingType()) {
            case BenchmarkConfiguration.NO_POOLING:
                pool.setPooling(false);
                pool.setStatementPooling(false);
                break;

            case BenchmarkConfiguration.CONNECTION_POOLING:
                pool.setPooling(true);
                pool.setStatementPooling(false);
                break;

            case BenchmarkConfiguration.STATEMENT_POOLING:
                pool.setPooling(true);
                pool.setStatementPooling(true);
                break;

            default:
                throw new SQLException("Pooling type unknown.");
        }

        
        return pool;
    }

}