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
import java.sql.SQLException;

/**
 * This is AS3AP benchmarking suite.
 */
public abstract class BenchmarkSuite extends TestSuite {
    
    public static final boolean CREATE_DATABASE = false;

    /** @todo make these params configurable */
    public static final String DATA_PATH = "../../as3ap/data-4mb";
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
    
    protected BenchmarkDatabaseManager createDatabaseManager() throws SQLException {
        return new BenchmarkDatabaseManager(
            DATABASE_PATH, USER_NAME, PASSWORD, isCreateDatabase());
    }
    
    /**
     * Get benchmark test suite that will be executed.
     * 
     * @return instance of {@link Test} containing the suite.
     */
    public Test suite() {
        
        fillSuite();
        
        TestSetup setup = new TestSetup(this) {
            
            protected void setUp() throws Exception {
                databaseManager = createDatabaseManager();
                    
                fixture = 
                    new BenchmarkFixture(databaseManager, new File(DATA_PATH));
                    
                if (isCreateDatabase())
                    fixture.createDatabase();
            }

            protected void tearDown() throws Exception {
                // do nothing, we disconnect automatically
            }
            
        };
        
        return setup;
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
    
    protected boolean isCreateDatabase() {
        return CREATE_DATABASE;
    }

    /**
     * Fill the test suite. This method is called during the suite construction.
     * Each subclass should fill the suite by calling {@link #addTest(Test)}
     * or {@link #addTestSuite(Class)} methods.
     */    
    public abstract void fillSuite();
    
}