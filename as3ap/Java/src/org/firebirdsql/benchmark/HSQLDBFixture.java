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
import java.sql.SQLException;

/**
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class HSQLDBFixture extends BenchmarkFixture {
    
    
    /**
     * Create instance of this class for the specified database manager
     * and data path.
     * 
     * @param manager instance of {@link BenchmarkDatabaseManager}.
     * @param dataPath path where data are located.
     */
    public HSQLDBFixture(File dataPath) throws SQLException {
        super(dataPath);
    }
    
    /**
     * Create database.
     */
    public void createDatabase() throws SQLException {
        
        executeDDL(BenchmarkDDL.CREATE_UPDATES_TABLE);
        executeDDL(BenchmarkDDL.CREATE_HUNDRED_TABLE);
        executeDDL(BenchmarkDDL.CREATE_HUNDRED_FOREIGN_KEY);
        executeDDL(BenchmarkDDL.CREATE_TEN_PCT_TABLE);
        executeDDL(BenchmarkDDL.CREATE_UNIQUES_TABLE);
        executeDDL(BenchmarkDDL.CREATE_TINY_TABLE);
        
        executeDDL(BenchmarkDDL.CREATE_TINY_OUTPUT);
        executeDDL(BenchmarkDDL.CREATE_UPDATES_OUTPUT);
        executeDDL(BenchmarkDDL.CREATE_HUNDRED_OUTPUT);
        
        executeDDL(BenchmarkDDL.CREATE_SAVE_UPDATES_TABLE);
        
        executeDDL(BenchmarkDDL.CREATE_SEL_100_SEQ_TABLE);
        executeDDL(BenchmarkDDL.CREATE_SEL_100_RND_TABLE);
    }
    
    /**
     * Execute DDL statement. This code converts CREATE TABLE statement into
     * CREATE CACHED TABLE statements, so HSQLDB writes all data on disk making
     * tests more fair.
     * 
     * @param statement statement to execute.
     * 
     * @throws SQLException if SQL error occured during execution.
     */
    private void executeDDL(String statement) throws SQLException {
        String createTableStr = "CREATE TABLE";
        int createTableIndex = statement.indexOf(createTableStr);
        
        BenchmarkConfiguration config = getConfig();
        boolean useCachedTables = config.getCustomBooleanProperty("hsqldb.cached", true);
        
        String newStatement;
        if (createTableIndex != -1 && useCachedTables) 
            newStatement = "CREATE CACHED TABLE" + 
                statement.substring(createTableIndex + createTableStr.length());
        else
            newStatement = statement;
            
        getManager().executeDDL(newStatement);
    }

    /**
     * Shutdown this database.
     */
    public void tearDown(boolean dropDatabase) throws SQLException {
        getManager().executeDDL("SHUTDOWN");
        super.tearDown(dropDatabase);
    }

    /**
     * Create instance of {@link BenchmarkDatabaseManager} that will provide
     * access to the database. 
     * 
     * @return instance of {@link BenchmarkDatabaseManager}.
     * 
     * @throws SQLException if something went wrong.
     */
    public BenchmarkDatabaseManager createDatabaseManager() throws SQLException {
        return new JdbcDatabaseManager();
    }
}
