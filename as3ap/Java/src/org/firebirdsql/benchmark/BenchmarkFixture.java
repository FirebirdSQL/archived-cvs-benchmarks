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

import java.io.*;
import java.util.StringTokenizer;
import java.sql.*;
import java.util.Date;

import junit.framework.Test;

/**
 * This is benchmark suite fixture.
 */
public abstract class BenchmarkFixture {
    
    private BenchmarkDatabaseManager manager;
    private File dataPath;
    
    public BenchmarkFixture(File dataPath) throws SQLException {
        this.manager = createDatabaseManager();
        this.dataPath = dataPath;
    }
    
    protected BenchmarkDatabaseManager getManager() {
        return manager;
    }
    
    /**
     * Get path to the initial data.
     * 
     * @return path to data.
     */
    public File getDataPath() {
        return dataPath;
    }
    
    /**
     * Set up benchmark suite. This method is called before test suite is
     * executed once.
     * 
     * @param createDatabase <code>true</code> if database has to be recreated.
     * 
     * @throws SQLException if database access error occurs.
     */
    public void setUp(boolean createDatabase) throws SQLException {
        if (createDatabase)
            createDatabase();
    }
    
    /**
     * Tear down the suite. This method is called once after the suite is
     * finished.
     * 
     * @param dropDatabase <code>true</code> if database must be dropped.
     * 
     * @throws SQLException if database access error occurs.
     */
    public void tearDown(boolean dropDatabase) throws SQLException {
        // do nothing here
    }
    
    public BenchmarkConfiguration getConfig() {
        return BenchmarkConfiguration.getConfiguration();
    }
    
    public abstract BenchmarkDatabaseManager createDatabaseManager() 
        throws SQLException;
    
    /**
     * Create new data load test. This method can be ovverwritten by the subclass
     * to provide adapted version of the test case.
     * 
     * @param testName name of the test case.
     * 
     * @return instance of {@link TestCase}
     */
    protected Test createLoadTest(String testName) {
        return new LoadTest(testName);
    }
    
    protected Test createOutputTest(String name) {
        return new OutputTest(name);
    }
    
    protected Test createSelectTest(String name) {
        return new SelectTest(name);
    }
    
    protected Test createJoinTest(String name) {
        return new JoinTest(name);
    }
    
    protected Test createProjectionTest(String name) {
        return new ProjectionTest(name);
    }
    
    protected Test createAggregateTest(String name) {
        return new AggregateTest(name);
    }

    protected Test createIndexTest(String name) {
        return new IndexTest(name);
    }
    
    protected Test createUpdateTest(String name) {
        return new UpdateTest(name);
    }
    
    protected BackgroundMultiUserTest createBackgroundMultiUserTest(String name, int keyRange) {
        return new BackgroundMultiUserTest(name, keyRange);
    }
    
    protected MainstreamMultiUserTest createMainstreamMultiUserTest(String name, int keyRange, int duration) {
        return new MainstreamMultiUserTest(name, keyRange, duration);
    }

    
    /**
     * Create database and execute all necessary DDL statements.
     * 
     * @throws SQLException if something went wrong.
     */
    public void createDatabase() throws SQLException {
        manager.executeDDL(BenchmarkDDL.CREATE_UPDATES_TABLE);
        manager.executeDDL(BenchmarkDDL.CREATE_HUNDRED_TABLE);
        manager.executeDDL(BenchmarkDDL.CREATE_HUNDRED_FOREIGN_KEY);
        manager.executeDDL(BenchmarkDDL.CREATE_TEN_PCT_TABLE);
        manager.executeDDL(BenchmarkDDL.CREATE_UNIQUES_TABLE);
        manager.executeDDL(BenchmarkDDL.CREATE_TINY_TABLE);
        
        manager.executeDDL(BenchmarkDDL.CREATE_TINY_OUTPUT);
        manager.executeDDL(BenchmarkDDL.CREATE_UPDATES_OUTPUT);
        manager.executeDDL(BenchmarkDDL.CREATE_HUNDRED_OUTPUT);
        
        manager.executeDDL(BenchmarkDDL.CREATE_SAVE_UPDATES_TABLE);
        
        manager.executeDDL(BenchmarkDDL.CREATE_SEL_100_SEQ_TABLE);
        manager.executeDDL(BenchmarkDDL.CREATE_SEL_100_RND_TABLE);
    }

    /**
     * Recreate updates table. This method is used to restore updates relation,
     * and drop/create sequence works better than deleting a content from the
     * relation due to garbage collection.
     * 
     * @throws SQLException if something went wrong.
     */    
    public void recreateUpdates() throws SQLException {
        /*
        // this causes "object in use error", so we will simply delete content
        manager.executeDDL(BenchmarkDDL.DROP_HUNDRED_FOREIGN_KEY);
        manager.executeDDL(BenchmarkDDL.DROP_UPDATES_TABLE);
        manager.executeDDL(BenchmarkDDL.CREATE_UPDATES_TABLE);
        manager.executeDDL(BenchmarkDDL.CREATE_HUNDRED_FOREIGN_KEY);
        */
        
        // manager.executeDDL("DELETE FROM " + BenchmarkDDL.UPDATES_TABLE);
    }
    
    /**
     * Recreate temporary updates tables. 
     * 
     * @throws SQLException if something went wrong.
     */
    public void recreateTempUpdates() throws SQLException {
        /* 
        // this causes "object in use error", so we will simply delete content
        manager.executeDDL(BenchmarkDDL.DROP_SEL_100_SEQ_TABLE);
        manager.executeDDL(BenchmarkDDL.DROP_SEL_100_RND_TABLE);
        manager.executeDDL(BenchmarkDDL.CREATE_SEL_100_SEQ_TABLE);
        manager.executeDDL(BenchmarkDDL.CREATE_SEL_100_RND_TABLE);
        */
        
        manager.executeDDL("DELETE FROM " + BenchmarkDDL.SEL_100_SEQ_TABLE);
        manager.executeDDL("DELETE FROM " + BenchmarkDDL.SEL_100_RND_TABLE);
    }

    /**
     * Load data in CSV format from the specified file using the specified
     * insert statement.
     * 
     * @param file file from which data should be loaded.
     * @param insertSql SQL statement that will be used to insert data.
     * 
     * @throws SQLException if something went wrong.
     */
    public void loadFile(File file, String insertSql) 
        throws SQLException, IOException 
    {
        Connection connection = manager.getConnection();
        connection.setAutoCommit(false);
        
        try {
            loadFile(file, connection, insertSql);
            connection.commit();
        } catch(SQLException ex) {
            connection.rollback();
        } finally {
            connection.close();
        }
    }
        
    public void loadFile(File file, Connection connection, String insertSql)
        throws SQLException, IOException 
    {
        System.out.println("Loading file " + file.getName());

        LineNumberReader in = new LineNumberReader(
            new InputStreamReader(new FileInputStream(file)));


        PreparedStatement stmt = null;

        try {
            stmt = connection.prepareStatement(insertSql);

            int rowCount = 0;
            String line = null;
            while((line = in.readLine()) != null) {

                StringTokenizer st = new StringTokenizer(line, ",");

                int counter = 1;
                while(st.hasMoreTokens()) {
                    if (counter != 7)
                        stmt.setString(counter++, st.nextToken());
                    else {
                        Date date = new Date(st.nextToken());
                        Timestamp timestamp = new Timestamp(date.getTime());
                        stmt.setTimestamp(counter++, timestamp);
                    }
                }
                
                if (rowCount != 0 && rowCount % 1000 == 0)
                    System.out.println("Inserted " + rowCount + " rows");

                stmt.executeUpdate();
                rowCount++;

            }
        } finally {
            if (stmt != null)
                stmt.close();
        }
    }
    
    /**
     * Convert array of strings into comma-separated string.
     * 
     * @param strings strings to convert.
     * 
     * @return instance of {@link String} where strings are comma-separated.
     */
    public static String toCSVString(String[] strings) {
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < strings.length; i++) {
            sb.append(strings[i]);
            if (i < strings.length - 1)
                sb.append(", ");
        }

        return sb.toString();
    }
    
    
}