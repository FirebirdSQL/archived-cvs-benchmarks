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
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This is a base class for all benchmark tests.
 */
public class BenchmarkTest extends BenchmarkDDL {

    public BenchmarkTest(String name) {
        super(name);
    }
    
    private BenchmarkFixture fixture;
    private BenchmarkDatabaseManager databaseManager;
    private Connection connection;

    protected void setUp() throws Exception {
        databaseManager = BenchmarkSuite.getDatabaseManager();
        fixture = BenchmarkSuite.getFixture();
        
        connection = databaseManager.getConnection();
    }

    protected void tearDown() throws Exception {
        connection.close();
    }
    
    protected BenchmarkFixture getFixture() {
        return fixture;
    }
    
    protected BenchmarkDatabaseManager getDatabaseManager() {
        return databaseManager;
    }
    
    protected Connection getConnection() {
        return connection;
    }

    /**
     * This class can fetch complete result set to the client. 
     */
    protected static class Fetcher {

        private int colNum;

        protected Fetcher(String[] columns) {
            this.colNum = columns.length;
        }
        
        protected Fetcher(int colNum) {
            this.colNum = colNum;
        }

        protected void fetchResultSet(ResultSet rs) throws SQLException {
            while(rs.next()) {
                for (int i = 0; i < colNum; i++) { 
                    Object obj = rs.getObject(i + 1);
                }
            }
        }
    }
}