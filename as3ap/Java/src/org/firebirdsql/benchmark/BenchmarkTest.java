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

import junit.framework.*;
import java.sql.Connection;
import java.io.File;

/**
 * This is a base class for all benchmark tests.
 */
public class BenchmarkTest extends BenchmarkDDL {

    public static final String HUNDRED_FILE = "asap.hundred";
    public static final String TEN_PCS_FILE = "asap.tenpct";
    public static final String TINY_FILE = "asap.tiny";
    public static final String UNIQUES_FILE = "asap.uniques";
    public static final String UPDATES_FILE = "asap.updates";
    
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

    public void testLoadData() throws Exception {
        
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
}