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

import junit.framework.TestSuite;

/**
 * This class represents a single-user test suite from AS3AP bechmark.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class SingleUserSuite extends BenchmarkSuite {

    /**
     * Fill the suite. Depending on the result of {@link #isCreateDatabase()}
     * method, database filling test is added or not.
     */
    public void fillSuite() {
        // add tests to the test suite here
        
        if (CREATE_DATABASE)
            addTest(new LoadTest("testLoadData"));
        
        addTest(getSingleUserTests());
    }
    
    /**
     * Get all single-user tests except loading the data into the database.
     * 
     * @return instance of {@link TestSuite} ready for execution.
     */
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
     * Run this benchmark suite.
     * 
     * @param args arguments to this program.
     */
    public static void main(String[] args) {
        BenchmarkListener listener = new BenchmarkListener();
        TestRunner.run(new SingleUserSuite().suite(), listener);
        listener.printStatistics(System.out);
    }

}
