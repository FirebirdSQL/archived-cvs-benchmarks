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
import org.firebirdsql.gds.*;
import org.firebirdsql.jdbc.FBSQLException;
import org.firebirdsql.jdbc.FBSimpleDataSource;

/**
 * This class is responsible for database management.
 * 
 */
public class BenchmarkDatabaseManager {
    
    private String database;
    private String userName;
    private String password;
    
    private FBSimpleDataSource dataSource;
    
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
    public BenchmarkDatabaseManager(String database, String userName, 
        String password, boolean create) throws SQLException 
    {
        this.database = database;
        this.userName = userName;
        this.password = password;
        
        if (create)
            createDatabase();
            
        dataSource = new FBSimpleDataSource();
        dataSource.setDatabase(database);
        dataSource.setUserName(userName);
        dataSource.setPassword(password);
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
            userName)
        );
        
        dpb.append(GDSFactory.newClumplet(
            ISCConstants.isc_dpb_password, 
            password)
        );
        
        isc_db_handle db = gds.get_new_isc_db_handle();
        
        try {
            gds.isc_create_database(database, db, dpb);
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
        return dataSource.getConnection();
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
                connection.close();
            }
        }
    }
}