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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Fixture for DaffodilDB.
 */
public class DaffodilDbFixture extends BenchmarkFixture {

    /**
     * Create instance of this fixture for the specified path.
     * 
     * @param dataPath path to the data.
     * 
     * @throws SQLException if something went wrong.
     */
    public DaffodilDbFixture(File dataPath) throws SQLException {
        super(dataPath);
    }
    
    /* (non-Javadoc)
     * @see org.firebirdsql.benchmark.BenchmarkFixture#createDatabase()
     */
    public void createDatabase() throws SQLException {
        super.createDatabase();
    }
    /* (non-Javadoc)
     * @see org.firebirdsql.benchmark.BenchmarkFixture#createDatabaseManager()
     */
    public BenchmarkDatabaseManager createDatabaseManager() throws SQLException {
        return new DaffodilDBManager();
    }

    private static class DaffodilDBManager extends BenchmarkDatabaseManager {

        private static final String JDBC_DRIVER_NAME = "in.co.daffodil.db.jdbc.DaffodilDBDriver";
        
        private String jdbcUrl;
        private String userName;
        private String password;
        
        /**
         * Create instance of this class.
         * 
         * @throws SQLException if instance cannot be created.
         */
        public DaffodilDBManager() throws SQLException {
            super();
            
            try {
                Class.forName(JDBC_DRIVER_NAME);
            } catch(ClassNotFoundException ex) {
                throw new SQLException("Cannot find DaffodilDB JDBC driver in classpath.");
            }
            
            BenchmarkConfiguration config = BenchmarkConfiguration.getConfiguration();
            
            boolean createDb = config.getCustomBooleanProperty("daffodil.create", false);
            String path = config.getCustomProperty("daffodil.path");
            String name = config.getCustomProperty("daffodil.name");
            
            userName = config.getUserName();
            password = config.getPassword();
            
            if (name == null)
                throw new SQLException("Property custom.daffodil.name is required");
            
            StringBuffer sb = new StringBuffer();
            
            sb.append("jdbc:daffodilDB_embedded:");
            sb.append(name);
            
            if (path != null)
                sb.append(";").append("path=").append(path);
            
            if (createDb)
                sb.append(";").append("create=true");
            
            jdbcUrl = sb.toString();
        }
        /* (non-Javadoc)
         * @see org.firebirdsql.benchmark.BenchmarkDatabaseManager#dropTable(java.lang.String)
         */
        public void dropTable(String tableName) throws SQLException {
            Connection connection = getConnection();
            try {
                Statement stmt = connection.createStatement();
                try {
                    stmt.execute("DROP TABLE " + tableName + " CASCADE");
                } finally {
                    stmt.close();
                }
            } finally {
                releaseConnection(connection);
            }
        }
        
        /* (non-Javadoc)
         * @see org.firebirdsql.benchmark.BenchmarkDatabaseManager#getConnection()
         */
        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(jdbcUrl, userName, password);
        }
    }
}
