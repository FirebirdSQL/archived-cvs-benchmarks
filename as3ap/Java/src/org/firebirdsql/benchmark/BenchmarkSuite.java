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
import junit.extensions.TestSetup;
import java.io.File;

/**
 * This is AS3AP benchmarking suite.
 */
public class BenchmarkSuite {

    /** @todo make these params configurable */
    public static final String DATA_PATH = "./../data-4mb";
    public static final String DATABASE_PATH = "localhost/3050:d:/database/as3ap.gdb";
    public static final String USER_NAME = "SYSDBA";
    public static final String PASSWORD = "masterkey";
        
    private static BenchmarkDatabaseManager databaseManager;
    private static BenchmarkFixture fixture;
    
    public static BenchmarkDatabaseManager getDatabaseManager() {
        return databaseManager;
    }
    
    public static BenchmarkFixture getFixture() {
        return fixture;
    }
    
    /**
     * Get benchmark test suite that will be executed.
     * 
     * @return instance of {@link Test} containing the suite.
     */
    public static Test suite() {
        
        TestSuite suite = new TestSuite();
        
        fillSuite(suite);
        
        TestSetup setup = new TestSetup(suite) {
            
            protected void setUp() throws Exception {
                databaseManager = new BenchmarkDatabaseManager(
                    DATABASE_PATH, USER_NAME, PASSWORD, true);
                    
                fixture = 
                    new BenchmarkFixture(databaseManager, new File(DATA_PATH));
                    
                fixture.createDatabase();
            }

            protected void tearDown() throws Exception {
                // do nothing, we disconnect automatically
            }
            
        };
        
        return setup;
    }
    
    public static void fillSuite(TestSuite suite) {
        // add tests to the test suite here
        
        suite.addTest(new BenchmarkTest("testLoadData"));
    }
    
    public static void main(String[] args) {
        TestRunner.run(suite(), new BenchmarkListener());
    }
}