/*
 * Firebird Open Source AS3AP Benchmark suite
 *
 * Distributable under IBM Public License. You may obtain a copy of the License 
 * at http://www.opensource.org/licenses/ibmpl.php
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  
 *
 * File contains portions of junit.extensions.ActiveTestSuite code and remains 
 * copyrighted work of  junit.extensions.ActiveTestSuite author(s).
 *
 * Copyright (C) 1996, 1999 International Business Machines Corporation and 
 * others. All Rights Reserved.
 */
package org.firebirdsql.benchmark;

import junit.extensions.ActiveTestSuite;
import junit.framework.TestResult;
import java.util.Enumeration;
import junit.framework.Test;

/**
 * This class modifies {@link ActiveTestSuite} test suite by allowing returning
 * from {@link TestSuite#run(TestResult)} right after starting all threads.
 * This allows us running background tests in parallel to the mainstream tests.
 */
public class ActiveBenchmarkSuite extends ActiveTestSuite {

    private volatile int fActiveTestDeathCount;
    
    public void run(TestResult result) {
        for (Enumeration e= tests(); e.hasMoreElements(); ) {
              if (result.shouldStop() )
                  break;
            Test test= (Test)e.nextElement();
            runTest(test, result);
        }
    }

    public synchronized void waitSuiteCompletion() {
        while (fActiveTestDeathCount < testCount()) {
            try {
                wait();
            } catch (InterruptedException e) {
                return; // ignore
            }
        }
    }

    synchronized public void runFinished(Test test) {
        fActiveTestDeathCount++;
        notifyAll();
    }

}