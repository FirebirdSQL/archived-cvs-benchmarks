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
public class BenchmarkSuite extends TestSuite {
    
    public static final boolean CREATE_DATABASE = false;

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
    
    protected BenchmarkDatabaseManager createDatabaseManager() throws SQLException {
        return new BenchmarkDatabaseManager(
            DATABASE_PATH, USER_NAME, PASSWORD, CREATE_DATABASE);
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
                    
                if (CREATE_DATABASE)
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

    
    public void fillSuite() {
        // add tests to the test suite here
        
        if (CREATE_DATABASE)
            addTest(new LoadTest("testLoadData"));
        
        addTest(getSingleUserTests());
        addTest(getMultiuserTests());
    }
    
    protected TestSuite getSingleUserTests() {
        TestSuite suite = new TestSuite();
        
        suite.addTest(createOutputTest("testModeTinyRelation"));

        suite.addTest(createIndexTest("testTenPctKeyCodeIndex"));
        
        suite.addTest(createOutputTest("testModeTinyFile"));
        
        suite.addTest(createSelectTest("testSelect1Clustered"));
        
        suite.addTest(createOutputTest("testModeTinyScreen"));
        
        suite.addTest(createIndexTest("testTenPctIntIndex"));
        
        suite.addTest(createOutputTest("testMode100kFile"));
        suite.addTest(createOutputTest("testMode1kRelation"));
        
        suite.addTest(createIndexTest("testTenPctSignedIndex"));
        suite.addTest(createIndexTest("testUniquesCodeIndex"));
        
        suite.addTest(createOutputTest("testMode1kFile"));
        
        suite.addTest(createIndexTest("testTenPctDoubleIndex"));
        suite.addTest(createIndexTest("testUpdatesDecimIndex"));
        
        suite.addTest(createOutputTest("testMode10kFile"));
        
        suite.addTest(createIndexTest("testTenPctFloatIndex"));
        suite.addTest(createIndexTest("testUpdatesIntIndex"));
        suite.addTest(createIndexTest("testTenPctDecimIndex"));
        suite.addTest(createIndexTest("testHundredCodeIndex"));
        
        suite.addTest(createOutputTest("testMode10kRelation"));
        
        suite.addTest(createIndexTest("testTenPctNameIndex"));
        suite.addTest(createIndexTest("testUpdatesCodeIndex"));
        
        suite.addTest(createOutputTest("testMode100kRelation"));
        
        suite.addTest(createIndexTest("testTenPctCodeIndex"));
        suite.addTest(createIndexTest("testUpdatesDoubleIndex"));
        
        suite.addTest(createOutputTest("testMode1kScreen"));
        suite.addTest(createOutputTest("testMode10kScreen"));
        suite.addTest(createOutputTest("testMode100kScreen"));
        
        suite.addTest(createJoinTest("testJoin3Clustered"));
        suite.addTest(createSelectTest("testSelect100NonClustered"));
        /* @todo move this test case to a separate class */
        suite.addTest(createSelectTest("testTableScan"));
        suite.addTest(createAggregateTest("testFunctionAggregate"));
        suite.addTest(createAggregateTest("testScalarAggregate"));
        suite.addTest(createSelectTest("testSelect100Clustered"));
        suite.addTest(createJoinTest("testJoin3NonClustered"));
        suite.addTest(createSelectTest("testSelect10TenPctNonClustered"));
        suite.addTest(createAggregateTest("testSimpleReport"));
        suite.addTest(createAggregateTest("testInfoRetrieval"));
        suite.addTest(createAggregateTest("testCreateView"));
        suite.addTest(createAggregateTest("testSubtotalReport"));
        suite.addTest(createAggregateTest("testTotalReport"));
        suite.addTest(createJoinTest("testJoin2Clustered"));
        suite.addTest(createJoinTest("testJoin2"));
        suite.addTest(createSelectTest("testVariableSelectivity"));
        suite.addTest(createJoinTest("testJoin4Clustered"));
        suite.addTest(createProjectionTest("testProjection100"));
        suite.addTest(createJoinTest("testJoin4NonClustered"));
        suite.addTest(createProjectionTest("testProjectionTenPct"));
        suite.addTest(createSelectTest("testSelect1NonClustered"));
        suite.addTest(createJoinTest("testJoin2NonClustered"));
        
        
        suite.addTest(createUpdateTest("testIntegrity"));
        
        suite.addTest(createIndexTest("testUpdatesDropIndices"));
        
        suite.addTest(createUpdateTest("testBulkSave"));
        suite.addTest(createUpdateTest("testBulkModify"));
        
        suite.addTest(createUpdateTest("testAppendMiddle"));
        suite.addTest(createUpdateTest("testUpdateMiddle"));
        suite.addTest(createUpdateTest("testDeleteMiddle"));
        
        suite.addTest(createUpdateTest("testAppendEnd"));
        suite.addTest(createUpdateTest("testUpdateEnd"));
        suite.addTest(createUpdateTest("testDeleteEnd"));
        
        suite.addTest(createIndexTest("testUpdatesCodeIndex"));
        
        suite.addTest(createUpdateTest("testAppendMiddle"));
        suite.addTest(createUpdateTest("testUpdateMiddleCode"));
        suite.addTest(createUpdateTest("testDeleteMiddle"));
        
        suite.addTest(createIndexTest("testUpdatesIntIndex"));

        suite.addTest(createUpdateTest("testAppendMiddle"));
        suite.addTest(createUpdateTest("testUpdateMiddleInt"));
        suite.addTest(createUpdateTest("testDeleteMiddle"));
        
        suite.addTest(createUpdateTest("testBulkAppend"));
        suite.addTest(createUpdateTest("testBulkDelete"));
        
        return suite;
    }
    
    /**
     * Get multiuser test suite. According to the AS3AP suite following tests
     * are executed in emulated multi-user environment:
     * <ul>
     * <li>o_mode_tiny
     * <li>o_mode_100k
     * <li>select_1_ncl
     * <li>simple_report
     * <li>sel_100_seq
     * <li>sel_100_rand
     * <li>mod_100_seq_abort
     * <li>mod_100_rand
     * <li>unmod_100_seq
     * <li>unmod_100_rand
     * </ul>
     * 
     * @return instance of {@link TestSuite} containing multiuser tests.
     */
    protected TestSuite getMultiuserTests() {
        TestSuite suite = new TestSuite();
        
        /* @todo fill test suite here */

        return suite;
    }
    
    public static void main(String[] args) {
        BenchmarkListener listener = new BenchmarkListener();
        TestRunner.run(new BenchmarkSuite().suite(), listener);
    }
}