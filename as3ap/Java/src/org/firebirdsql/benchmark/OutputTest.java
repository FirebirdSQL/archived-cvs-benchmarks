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
import java.io.PrintStream;
import java.io.File;
import java.io.FileOutputStream;

/**
 * This class implements output tests from the AS3AP test suite.
 */
public abstract class OutputTest extends BenchmarkTest {
    
    public static final String O_MODE_TINY_SQL = ""
        + "SELECT * FROM " + TINY_TABLE
        ;
        
    public static final String O_MODE_1K_SQL = ""
        + "SELECT * FROM " + UPDATES_TABLE
        + " WHERE " + KEY_COL + " <= 10"
        ;
        
    public static final String O_MODE_10K_SQL = ""
        + "SELECT * FROM " + HUNDRED_TABLE
        + " WHERE " + KEY_COL + " <= 100"
        ;
        
    public static final String O_MODE_100K_SQL = ""
        + "SELECT * FROM " + HUNDRED_TABLE
        + " WHERE " + KEY_COL + " <= 1000"
        ;
        
    public OutputTest(String name) {
        super(name);
    }
    
    /**
     * This method executes query and outputs it to the specified stream.
     * 
     * @param sql query to execute.
     * 
     * @throws Exception if something went wrong.
     */
    protected void executeQueryToStream(String sql, PrintStream out) throws Exception {
        Statement stmt = getConnection().createStatement();

        ResultSet rs = stmt.executeQuery(sql);

        int columnCount = rs.getMetaData().getColumnCount();

        while (rs.next()) {
            StringBuffer sb = new StringBuffer();
            
            for (int i = 0; i < columnCount; i++) {
                sb.append(rs.getString(i + 1));
                
                if (i < columnCount - 1)
                    sb.append(",");
            }
            
            out.println(sb.toString());
        }
    }
    
    /**
     * This method executes query and outputs it to the screen.
     * 
     * @param sql query to execute.
     * 
     * @throws Exception if something went wrong.
     */
    protected void executeQueryToScreen(String sql) throws Exception {
        executeQueryToStream(sql, System.out);
    }
    
    /**
     * This method executes query and outputs it to the temp file.
     * 
     * @param sql query to execute.
     * 
     * @throws Exception if something went wrong.
     */
    protected void executeQueryToFile(String sql) throws Exception {
        File tempFile = File.createTempFile("as3ap", "out");
        
        try {
            PrintStream out = new PrintStream(new FileOutputStream(tempFile));
            
            try {
                executeQueryToStream(sql, out);
            } finally {
                out.close();
            }
            
        } finally {
            tempFile.delete();
        }
    }
    
    /**
     * This method executes statements "INSERT INTO ... SELECT * FROM ..."
     * 
     * @param sql statement to execute.
     * 
     * @throws Exception if something went wrong.
     */
    protected void executeQueryToRelation(String sql) throws Exception {
        Statement stmt = getConnection().createStatement();
        
        stmt.executeUpdate(sql);
    }
    
    /*
     * Output tests with screen
     */
     
    public void testModeTinyScreen() throws Exception {
        executeQueryToScreen(O_MODE_TINY_SQL);
    }
    
    public void testMode1kScreen() throws Exception {
        executeQueryToScreen(O_MODE_1K_SQL);
    }
    
    public void testMode10kScreen() throws Exception {
        executeQueryToScreen(O_MODE_10K_SQL);
    }
    
    public void testMode100kScreen() throws Exception {
        executeQueryToScreen(O_MODE_100K_SQL);
    }
    
    /*
     * Output tests with relations
     */
     
    public void testModeTinyRelation() throws Exception {
        executeQueryToRelation(
            "INSERT INTO " + TINY_TABLE_OUTPUT + " " + O_MODE_TINY_SQL);
    }
    
    public void testMode1kRelation() throws Exception {
        executeQueryToRelation(
            "INSERT INTO " + UPDATES_TABLE_OUTPUT + " " + O_MODE_1K_SQL);
    }
    
    public void testMode10kRelation() throws Exception {
        executeQueryToRelation(
            "INSERT INTO " + HUNDRED_TABLE_OUTPUT + " " + O_MODE_10K_SQL);
    }
    
    public void testMode100kRelation() throws Exception {
        executeQueryToRelation(
            "INSERT INTO " + HUNDRED_TABLE_OUTPUT + " " + O_MODE_100K_SQL);
    }
    
    /*
     * Output tests with file
     */

    public void testModeTinyFile() throws Exception {
        executeQueryToFile(O_MODE_TINY_SQL);
    }

    public void testMode1kFile() throws Exception {
        executeQueryToFile(O_MODE_1K_SQL);
    }

    public void testMode10kFile() throws Exception {
        executeQueryToFile(O_MODE_10K_SQL);
    }

    public void testMode100kFile() throws Exception {
        executeQueryToFile(O_MODE_100K_SQL);
    }
    
}