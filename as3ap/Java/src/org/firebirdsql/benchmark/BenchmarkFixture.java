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

/**
 * This is benchmark suite fixture.
 */
public class BenchmarkFixture {
    
    private BenchmarkDatabaseManager manager;
    private File dataPath;
    
    public BenchmarkFixture(BenchmarkDatabaseManager manager, File dataPath) {
        this.manager = manager;
        this.dataPath = dataPath;
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
     * Create database and execute all necessary DDL statements.
     * 
     * @throws SQLException if something went wrong.
     */
    public void createDatabase() throws SQLException {
        manager.createDatabase();
        
        manager.executeDDL(BenchmarkDDL.CREATE_UPDATES_TABLE);
        manager.executeDDL(BenchmarkDDL.CREATE_HUNDRED_TABLE);
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
        System.out.println("Loading file " + file.getName());

        LineNumberReader in = new LineNumberReader(
            new InputStreamReader(new FileInputStream(file)));

        Connection connection = manager.getConnection();

        try {
            connection.setAutoCommit(false);

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

                connection.commit();

            } catch(SQLException ex) {

                connection.rollback();

                throw ex;

            } finally {
                if (stmt != null)
                    stmt.close();
            }
        } finally {
            connection.close();
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