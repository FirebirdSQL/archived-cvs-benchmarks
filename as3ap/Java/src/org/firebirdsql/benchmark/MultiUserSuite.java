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


/**
 * This class represents multi-user test suite from the AS3AP benchmark.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class MultiUserSuite extends BenchmarkSuite {
    
    public static final int KEY_RANGE = 100 * 1000;
    
    private BenchmarkListener listener;
    
    public MultiUserSuite(BenchmarkListener listener) {
        this.listener = listener;
    }
    
    public BenchmarkListener getListener() {
        return listener;
    }
    
    protected int getUserCount() {
        return BenchmarkConfiguration.getConfiguration().getUserCount();
    }
    
    protected int getKeyRange() {
        return KEY_RANGE;
    }
    
    /** 
     * Fill this test suite.
     */
    public void fillSuite() {
        addTest(new Suite("testMultiUser"));
    }
    
    public boolean isCreateDatabase() {
        return false;
    }
    
    public class Suite extends TestCase {
        public Suite(String name) {
            super(name);
        }
        
        public void testMultiUser() throws Exception {
            
            TestResult testResult = new TestResult();
            testResult.addListener(listener);
            
            // Step 1
            
            System.out.println("step 1 completed");
            
            // Step 2
            
            BackgroundMultiUserTest[] bgTests = 
                new BackgroundMultiUserTest[getUserCount()];
                
            for (int i = 0; i < bgTests.length; i++) {
                bgTests[i] = new BackgroundMultiUserTest("testIrSelect", getKeyRange());
            }
            
            
            ActiveBenchmarkSuite bgIrSuite = new ActiveBenchmarkSuite();
            for (int i = 0; i < bgTests.length; i++) {
                bgIrSuite.addTest(bgTests[i]);
            }
            
            bgIrSuite.run(testResult);
            
            Thread.sleep(getDatabaseManager().getConfig().getBackgroundTestDuration());
            
            System.out.println("step 2 completed");
            
            // Step 3          
            
            MainstreamMultiUserTest perfIrTest = 
                new MainstreamMultiUserTest("testIrSelect", getKeyRange(), 
                    getDatabaseManager().getConfig().getPerformanceDuration());
            
            perfIrTest.run(testResult);
            
            System.out.println("step 3 completed: " + perfIrTest.getThroughput() + " fetches");
            
            // Step 4
            bgTests[0].stop();
            
            Test crossSectionTest = 
                new MainstreamMultiUserTest("testCrossSection", getKeyRange(), 0);
            crossSectionTest.run(testResult);
            
            System.out.println("step 4 completed");
            
            // Step 5
            for (int i = 0; i < bgTests.length; i++) {
                bgTests[i].stop();
            }
            
            bgIrSuite.waitSuiteCompletion();
            
            System.out.println("background suite stopped.");
            
            Test checkTest = 
                new MainstreamMultiUserTest("testCheck", 0, 0);
            checkTest.run(testResult);
            
            System.out.println("step 5 completed");
            
            // Step 6
            new LoadTest("testRestoreUpdates").run(testResult);
            
            System.out.println("step 6 completed");
            
            // Step 7
            getFixture().recreateTempUpdates();
            
            System.out.println("step 7 completed");
            
            // Step 8
            
            for (int i = 0; i < bgTests.length; i++) {
                bgTests[i] = new BackgroundMultiUserTest("testOltpUpdate", getKeyRange());
            }
            
            ActiveBenchmarkSuite bgOltpSuite = new ActiveBenchmarkSuite();
            for (int i = 0; i < bgTests.length; i++) {
                bgOltpSuite.addTest(bgTests[i]);
            }
            
            bgOltpSuite.run(testResult);
            
            Thread.sleep(getDatabaseManager().getConfig().getBackgroundTestDuration());
            
            System.out.println("step 8 completed");
            
            // Step 9
            perfIrTest.run(testResult);
            
            System.out.println("step 9 completed: " + perfIrTest.getThroughput());
            
            // Step 10
            bgTests[0].stop();;
            
            crossSectionTest.run(testResult);
            
            System.out.println("step 10 completed");
            
            // Step 11
            
            for (int i = 0; i < bgTests.length; i++) {
                bgTests[i].stop();
            }
            
            bgOltpSuite.waitSuiteCompletion();
            
            checkTest.run(testResult);
            
            System.out.println("step 11 completed");
        }
    }
    
    /**
     * Run this benchmark suite.
     * 
     * @param args arguments to this program.
     */
    public static void main(String[] args) {
        BenchmarkListener listener = new BenchmarkListener();
        MultiUserSuite suite = new MultiUserSuite(listener);
        TestRunner.run(suite.suite(), suite.listener);
        listener.printStatistics(System.out);
    }    
}
