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
import javax.sql.DataSource;
import javax.resource.ResourceException;
import org.firebirdsql.gds.*;
import org.firebirdsql.jdbc.FBSQLException;
import org.firebirdsql.jdbc.FBSimpleDataSource;
import org.firebirdsql.pool.FBConnectionPoolConfiguration;
import org.firebirdsql.pool.FBConnectionPoolDataSource;
import org.firebirdsql.pool.SimpleDataSource;
import org.firebirdsql.jdbc.FBWrappingDataSource;

/**
 * This class is responsible for database management.
 * 
 */
public class BenchmarkDatabaseManager {
    
    private DataSource dataSource;
    
    /**
     * Create instance of this class. 
     * 
     * @param database name of the database, e.g. 
     * <code>"localhost/3050:/tmp/benchmark/as3ap.gdb"</code>. Note, this is 
     * not JDBC URL.
     * 
     * @param userName user on behalf of whom test will be executed.
     * 
     * @param password password of the user.
     * 
     * @param create <code>true</code> if database should be created, otherwise
     * we assume that database was already created.
     * 
     * @throws SQLException if database cannot be created.
     */
    public BenchmarkDatabaseManager(boolean create) throws SQLException 
    {
        if (create)
            createDatabase();
        
        switch(getConfig().getPoolingType()) {
            case BenchmarkConfiguration.NO_POOLING :
                dataSource = getNoPoolingDataSource();
                break;
                
            case BenchmarkConfiguration.CONNECTION_POOLING :
                dataSource = getConnectionPoolingDataSource();
                break;
                
            case BenchmarkConfiguration.STATEMENT_POOLING :
                dataSource = getStatementPoolingDataSource();
                break;
                
            default :
                throw new SQLException("Pooling type unknown.");
        }
    }
    
    public BenchmarkConfiguration getConfig() {
        return BenchmarkConfiguration.getConfiguration();
    }
    
    protected DataSource getNoPoolingDataSource() throws SQLException {
        FBSimpleDataSource ds = new FBSimpleDataSource();
        
        try {
            ds.setTpbMapping(getConfig().getTpbMapping());
            ds.setDatabase(getConfig().getDatabasePath());
            ds.setUserName(getConfig().getUserName());
            ds.setPassword(getConfig().getPassword());
        } catch(ResourceException ex) {
            throw new FBSQLException(ex);
        }
        
        return ds;
    }

    protected DataSource getConnectionPoolingDataSource() throws SQLException {
        try {
            FBWrappingDataSource ds = new FBWrappingDataSource();
        
            ds.setTpbMapping(getConfig().getTpbMapping());
            ds.setDatabase(getConfig().getDatabasePath());
            ds.setUserName(getConfig().getUserName());
            ds.setPassword(getConfig().getPassword());
            ds.setPooling(true);
            ds.setMaxSize(getConfig().getMaxConnections());
        
            return ds;

        } catch(ResourceException ex) {
            throw new FBSQLException(ex);
        }
    }
    
    protected DataSource getStatementPoolingDataSource() throws SQLException {
        FBConnectionPoolConfiguration config = new FBConnectionPoolConfiguration();
        config.setJdbcUrl("jdbc:firebirdsql:" + getConfig().getDatabasePath());
        config.setProperty("user", getConfig().getUserName());
        config.setProperty("password", getConfig().getPassword());
        config.setProperty(
            "tpb_mapping",
            getConfig().getTpbMapping()
        );
        config.setMaxConnections(getConfig().getMaxConnections());
        
        FBConnectionPoolDataSource pool = new FBConnectionPoolDataSource(config);
        pool.start();
        
        return new SimpleDataSource(pool);
    }

    
    /**
     * Create database that was specified in a constructor. This method uses
     * GDS API directly, because FBManager methods throw instances of 
     * {@link Exception}, not {@link SQLException}.
     * 
     * @throws SQLException
     */
    protected void createDatabase() throws SQLException {
        
        GDS gds = GDSFactory.newGDS();
        
        // construct DPB
        Clumplet dpb = GDSFactory.newClumplet(ISCConstants.isc_dpb_version1);

        dpb = GDSFactory.newClumplet(
            ISCConstants.isc_dpb_num_buffers, 
            new byte[] {90}
        );
        
        dpb.append(GDSFactory.newClumplet(
            ISCConstants.isc_dpb_dummy_packet_interval, 
            new byte[] {120, 10, 0, 0})
        );
        
        dpb.append(GDSFactory.newClumplet(
            ISCConstants.isc_dpb_sql_dialect, 
            new byte[] {3, 0, 0, 0})
        );

        dpb.append(GDSFactory.newClumplet(
            ISCConstants.isc_dpb_user_name, 
            getConfig().getUserName())
        );
        
        dpb.append(GDSFactory.newClumplet(
            ISCConstants.isc_dpb_password, 
            getConfig().getPassword())
        );
        
        isc_db_handle db = gds.get_new_isc_db_handle();
        
        try {
            gds.isc_create_database(getConfig().getDatabasePath(), db, dpb);
            gds.isc_detach_database(db);
        } catch(GDSException ex) {
            throw new FBSQLException(ex);
        } 
        
        /*
        // this code is equivalent to the code above
        FBManager manager = new FBManager();
        manager.setForceCreate(true);
        
        manager.createDatabase(databaseURL, user, password);
        */
    }
    
    /**
     * Get connection to the benchmark database.
     * 
     * @return instance of {@link Connection}
     * 
     * @throws SQLException if connection cannot be obtained.
     */
    public Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        setUp(connection);
        return connection;
    }

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
    }
    
    /**
     * Tear down connection before releasing it.
     * 
     * @param connection connection to tear down.
     * 
     * @throws SQLException if something happened during connection tear down.
     */
    protected void tearDown(Connection connection) throws SQLException {
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