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

import junit.extensions.ActiveTestSuite;
import junit.framework.Test;
import junit.framework.TestResult;

/**
 * This class extends {@link ActiveTestSuite} test suite by allowing to specify
 * duration of the test execution. In this case each test from the test suite
 * will be started over and over again until the end of the test perios.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class ActiveBenchmarkSuite extends ActiveTestSuite {

    private long duration;
    
    /**
     * Create default instance of this class.
     */
    public ActiveBenchmarkSuite(){
        super();
    }

    /**
     * Create instance of this class with the specified duration.
     * 
     * @param duration duration of the test execution.
     */    
    public ActiveBenchmarkSuite(long duration) {
        this();
        
        this.duration = duration;
    }
    
    /**
     * Set duration of this test suite. If value is greater than 0, each
     * test case will be started over and over again until the desired test 
     * duration is reached.
     * 
     * @param duration duration of this test suite, cannot be negative.
     * 
     * @throws IllegalArgumentException if duration is negative. 0 means that
     * each test case will be run only once.
     */
    public void setDuration(long duration) {
        
        if (duration < 0)
            throw new IllegalArgumentException("Duration cannot be negative.");
        
        this.duration = duration;
    }
    
    /**
     * Run test. This method is called for each test case in a suite and runs
     * it once or repeatedly within the specified duration.
     */
    public void runTest(final Test test, final TestResult result) {
        Runnable runnable = new Runnable() {
            
            // copy duration, so that it cannot be changed later
            private long duration = ActiveBenchmarkSuite.this.duration;
            
            public void run() {
                try {
                    // ensure that with 0 duration test will be run only once
                    if (duration == 0)
                        test.run(result);
                    else {
                        long start = System.currentTimeMillis();
                        do {
                            test.run(result);
                        } while (System.currentTimeMillis() - start <= duration);
                    }
                } finally {
                    ActiveBenchmarkSuite.this.runFinished(test);
                }
            }
        };
        
        Thread t = new Thread(runnable, test.toString());
        t.start();
    }
    
}
