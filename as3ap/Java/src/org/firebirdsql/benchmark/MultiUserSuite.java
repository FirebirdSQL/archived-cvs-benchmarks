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

import sun.security.action.GetBooleanAction;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;


/**
 * This class represents multi-user test suite from the AS3AP benchmark.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class MultiUserSuite extends BenchmarkSuite {
    
    public static final int KEY_RANGE = 1000 * 1000 * 1000;

    protected int getUserCount() {
        return 10;
    }
    
    /** 
     * Fill this test suite.
     */
    public void fillSuite() {
        addTest(new Suite("testMultiUser"));
    }
    
    private class Suite extends TestCase {
        public Suite(String name) {
            super(name);
        }
        
        public void testMultiUser() throws Exception {
            
            TestResult testResult = new TestResult();
            
            // Step 1
            
            // Step 2
            ActiveBenchmarkSuite bgIrTests = new ActiveBenchmarkSuite(0, getUserCount());
            bgIrTests.addTest(new MultiUserTest("testIrSelect", KEY_RANGE));
            
            bgIrTests.run(testResult);
            
            Thread.sleep(1 * 60 * 1000);
            
            // Step 3            
            ActiveBenchmarkSuite perfIrTest = new ActiveBenchmarkSuite();
            perfIrTest.setDuration(1 * 60 * 1000);
            perfIrTest.addTest(new MultiUserTest("testIrSelect", KEY_RANGE));
            
            perfIrTest.run(testResult);
            perfIrTest.waitSuiteCompletion();
            
            // Step 4
            Thread victim = (Thread)bgIrTests.getThreads().get(0);
            victim.interrupt();
            
            victim.join();
            
            getCrossSectionSuite().run(testResult);
            
            // Step 5
            bgIrTests.stop();
            
            getCheckSuite().run(testResult);
            
            // Step 6
            new LoadTest("testRecoverUpdates").run(testResult);
            
            // Step 7
            getFixture().recreateTempUpdates();
            
            // Step 8
            ActiveBenchmarkSuite bgOltpTests = new ActiveBenchmarkSuite(0, getUserCount());
            bgOltpTests.addTest(new MultiUserTest("testOltpUpdate", KEY_RANGE));
            
            bgOltpTests.run(testResult);
            
            Thread.sleep(1 * 60 * 1000);
            
            // Step 9
            perfIrTest.run(testResult);
            perfIrTest.waitSuiteCompletion();
            
            // Step 10
            victim = (Thread)bgOltpTests.getThreads().get(0);
            victim.interrupt();
            
            victim.join();
            
            getCrossSectionSuite().run(testResult);
            
            // Step 12
            bgOltpTests.stop();
            
            getCheckSuite().run(testResult);
        }
    }
    
    private TestSuite getCrossSectionSuite() {
        TestSuite suite = new TestSuite();
        
        suite.addTest(new MultiUserTest("testModeTiny"));
        suite.addTest(new MultiUserTest("testMode100k"));
        suite.addTest(new MultiUserTest("testSelect1NonClustered"));
        suite.addTest(new MultiUserTest("testSimpleReport"));
        suite.addTest(new MultiUserTest("testSelect100Sequence"));
        suite.addTest(new MultiUserTest("testSelect100Random"));
        suite.addTest(new MultiUserTest("testModify100Sequence"));
        suite.addTest(new MultiUserTest("testModify100Random"));
        suite.addTest(new MultiUserTest("testUnmodify100Sequence"));
        suite.addTest(new MultiUserTest("testUnmodify100Random"));
        
        return suite;
    }
    
    private TestSuite getCheckSuite() {
        TestSuite suite = new TestSuite();
        
        suite.addTest(new MultiUserTest("testCheck100Random"));
        suite.addTest(new MultiUserTest("testCheck100Sequence"));
        
        return suite;
    }
}
