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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;

/**
 * This class extends {@link ActiveTestSuite} test suite by allowing to specify
 * duration of the test execution. In this case each test from the test suite
 * will be started over and over again until the end of the test perios.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class ActiveBenchmarkSuite extends TestSuite {

    private long duration = 0;
    private int threadCount = 1;
    
    private ArrayList threads = new ArrayList();
    /**
     * Create default instance of this class.
     */
    public ActiveBenchmarkSuite(){
        this(0, 1);
    }

    /**
     * Create instance of this class with the specified duration.
     * 
     * @param duration duration of the test execution.
     */    
    public ActiveBenchmarkSuite(long duration, int threadCount) {
        super();
        
        this.duration = duration;
        this.threadCount = threadCount;
    }
    
    /**
     * Set duration of this test suite. If value is greater than 0, each
     * test case will be started over and over again until the desired test 
     * duration is reached.
     * 
     * @param duration duration of this test suite, cannot be negative.
     * 
     * @throws IllegalArgumentException if duration is negative. 0 means that
     * each test case will be run forever.
     */
    public void setDuration(long duration) {
        
        if (duration < 0)
            throw new IllegalArgumentException("Duration cannot be negative.");
        
        this.duration = duration;
    }
    
    /**
     * Get duration of this test suite.
     * 
     * @return duration of this test suite, 0 if test suite is executed only 
     * once.
     */
    public long getDuration() {
        return duration;
    }
    
    /**
     * Set number of threads to run.
     * @param threadCount
     */
    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
    
    /**
     * Run test. This method is called for each test case in a suite and runs
     * it once or repeatedly within the specified duration.
     */
    public void runTest(final Test test, final TestResult result) {
        
        // define a method local class that will run the test 
        class TestRunner implements Runnable {
            
            // copy duration, so that it cannot be changed later
            private long duration = ActiveBenchmarkSuite.this.duration;
            
            public void run() {
                try {
                    
                    long start = System.currentTimeMillis();
                    boolean runFlag = true;
                    
                    // ensure that at least the test is executed once
                    do {
                        test.run(result);
                        
                        long elapsedTime = System.currentTimeMillis() - start;
                        runFlag = duration == 0 || elapsedTime <= duration;
                    } while (runFlag && !Thread.interrupted());
                    
                } finally {
                    ActiveBenchmarkSuite.this.runFinished(Thread.currentThread());
                }
            }
        };
        
        for(int i = 0; i < threadCount; i++) {
            Thread t = new Thread(new TestRunner(), test.toString() + "-" + i);
            threads.add(t);
        }
        
        Iterator iter = threads.iterator();
		while (iter.hasNext()) {
			Thread element = (Thread) iter.next();
			element.start();
		}
    }
    
    /**
     * Wait untill all threads are finished.
     */
    public synchronized void waitSuiteCompletion() {
        while(threads.size() > 0) {
            try {
                wait();
            } catch(InterruptedException ex) {
                // ignore
            }
        }
    }
    
    /**
     * Interrupt all currently running threads and wait until they finish.
     */
    public synchronized void stop() {
        Iterator iter = threads.iterator();
		while (iter.hasNext()) {
			Thread element = (Thread) iter.next();
			element.interrupt();
		}
        
        waitSuiteCompletion();
    }

    /**
     * Get all running threads. This method gives access to the list of currently
     * running threads.
     * 
     * @return list with all currently running threads.
     */
    public List getThreads() {
        return new ArrayList(threads);
    }

	/**
     * Remove finished thread and notify all about changed state.
	 */
	private synchronized void runFinished(Thread thread) {
		threads.remove(thread);
        notifyAll();
	}

}
