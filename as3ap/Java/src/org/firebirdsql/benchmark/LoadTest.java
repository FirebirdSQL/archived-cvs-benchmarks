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
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class implements data load tests from AS3AP test suite.
 */
public class LoadTest extends BenchmarkTest {
    
    public static final String HUNDRED_FILE = "asap.hundred";
    public static final String TEN_PCS_FILE = "asap.tenpct";
    public static final String TINY_FILE = "asap.tiny";
    public static final String UNIQUES_FILE = "asap.uniques";
    public static final String UPDATES_FILE = "asap.updates";

    public LoadTest(String name) {
        super(name);
    }
    
    public void testLoadData() throws Exception {
        
        BenchmarkFixture fixture = getFixture();

        File dataPath = fixture.getDataPath();

        fixture.loadFile(new File(dataPath, UPDATES_FILE), 
            BenchmarkInsertSQL.INSERT_UPDATES);

        fixture.loadFile(new File(dataPath, HUNDRED_FILE), 
            BenchmarkInsertSQL.INSERT_HUNDRED);

        fixture.loadFile(new File(dataPath, TEN_PCS_FILE), 
            BenchmarkInsertSQL.INSERT_TEN_PCT);

        fixture.loadFile(new File(dataPath, UNIQUES_FILE), 
            BenchmarkInsertSQL.INSERT_UNIQUES);

        fixture.loadFile(new File(dataPath, TINY_FILE), 
            BenchmarkInsertSQL.INSERT_TINY);
    }
    
    public void testBackupUpdates() throws Exception {
        // do nothing here
    }
    
    public void testRestoreUpdates() throws Exception {
        if (getDatabaseManager().getConfig().isRecreateTableAsCleanup())
            restoreUpdatesByDrop();
        else
            restoreUpdatesByDelete();
    }
    
    protected void restoreUpdatesByDelete() throws Exception {
        BenchmarkFixture fixture = getFixture();
        BenchmarkDatabaseManager manager = getDatabaseManager();
        
        Connection con = getConnection();
        try {
            con.setAutoCommit(false);
        
            Statement stmt = con.createStatement();
            
            stmt.executeUpdate("DELETE FROM " + HUNDRED_TABLE);
            stmt.executeUpdate("DELETE FROM " + UPDATES_TABLE);
            
            fixture.loadFile(new File(fixture.getDataPath(), UPDATES_FILE), 
               con, BenchmarkInsertSQL.INSERT_UPDATES);
               
            fixture.loadFile(new File(fixture.getDataPath(), HUNDRED_FILE), 
                con, BenchmarkInsertSQL.INSERT_HUNDRED);
               
            con.commit();
            
        } catch(SQLException ex) {
            con.rollback();
            
            throw ex;
        } 
    }
    
    protected void restoreUpdatesByDrop() throws Exception {
        BenchmarkFixture fixture = getFixture();
        BenchmarkDatabaseManager manager = getDatabaseManager();
        
        Connection con = getConnection();
        try {
            
            con.setAutoCommit(false);
        
            Statement stmt = con.createStatement();
            
            stmt.executeUpdate(DROP_HUNDRED_FOREIGN_KEY);
            stmt.executeUpdate(DROP_UPDATES_TABLE);
            stmt.executeUpdate(CREATE_UPDATES_TABLE);
            
            con.commit();
            
            fixture.loadFile(new File(fixture.getDataPath(), UPDATES_FILE), 
               con, BenchmarkInsertSQL.INSERT_UPDATES);
               
            stmt.executeUpdate(CREATE_HUNDRED_FOREIGN_KEY);
            con.commit();
            
        } catch(SQLException ex) {
            con.rollback();
            
            throw ex;
        }         
    }
    
}