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

import junit.framework.TestListener;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This is JUnit test listener that is able to collect test execution time.
 */
public class BenchmarkListener implements TestListener {
    
    private HashMap testTimes = new HashMap();
    private HashMap startTimes = new HashMap();
    
    private HashMap errors = new HashMap();
    private HashMap failures = new HashMap();
    private List executionTrace = new LinkedList();
    
    
    public synchronized void addError(Test test, Throwable throwable) {
        startTimes.remove(test);
        testTimes.remove(test);
        
        errors.put(test, throwable);
    }

    public synchronized void addFailure(Test test, AssertionFailedError assertionFailedError) {
        startTimes.remove(test);
        testTimes.remove(test);
        
        failures.put(test, assertionFailedError);
    }

    private String getTestName(Test test) {
        if (test instanceof TestCase)
            return ((TestCase)test).getName();
        else
            return test.toString();
    }

    public synchronized void endTest(Test test) {

        Long start = (Long)startTimes.get(test);

        if (start != null) {
            testTimes.put(test, new Long(
                System.currentTimeMillis() - start.longValue()));

            startTimes.remove(test);
        }
    }

    public synchronized void startTest(Test test) {
        executionTrace.add(test);
        startTimes.put(test, new Long(System.currentTimeMillis()));
    }
    
    public synchronized void printStatistics(PrintStream out) {
        out.println("Test execution statistics:");
        
        HashMap stats = new HashMap();
        
        Iterator iter = executionTrace.iterator();
        while(iter.hasNext()) {
            Test test = (Test)iter.next();
            
            String testName = getTestName(test);            
            
            Long testTime = (Long)testTimes.get(test);

            AssertionFailedError failure = 
                (AssertionFailedError)failures.get(test);

            Throwable error = (Throwable)errors.get(test);
            
            StatisticsEntry entry = (StatisticsEntry)stats.get(testName);
            
            if (entry == null) {
                entry = new StatisticsEntry();
                entry.testName = testName;
                stats.put(test, entry);
            }
            
            entry.totalCount++;
            
            if (testTime != null)
                entry.totalDuration += testTime.longValue();
            
            if (error != null)
                entry.errors.add(error);
                
            if (failure != null)
                entry.failures.add(failure);
            
        }
        
        Iterator statsIter = executionTrace.iterator();
		while (statsIter.hasNext()) {
            Test test = (Test)statsIter.next();

			StatisticsEntry element = (StatisticsEntry) stats.get(test);
            if (element != null)
    			element.print(out);
		}
    }
    
    private static class StatisticsEntry {
        private String testName;
        private long totalDuration;
        private int totalCount;
        private HashSet failures = new HashSet();
        private HashSet errors = new HashSet();
        
        private void print(PrintStream out) {
            StringBuffer msg = new StringBuffer();
            
            msg.append(testName).append(" - ");
            msg.append(totalCount).append(" time(s)");
            msg.append(", ");
            msg.append("in ").append(totalDuration).append(" ms.");
            
            if (failures.size() > 0) {
                msg.append(", ");
                msg.append(failures.size()).append(" failures");
            }
            
            if (errors.size() > 0) {
                msg.append(", ");
                msg.append(errors.size()).append(" errors");
            }
            
            msg.append(".");
            
            if (failures.size() > 0 || errors.size() > 0) {
                
                Iterator iter = failures.iterator();
				while (iter.hasNext()) {
                    AssertionFailedError element = (AssertionFailedError)iter.next();
					msg.append("\n").append(element.toString());
				}
                
                iter = errors.iterator();
                while (iter.hasNext()) {
                    Throwable element = (Throwable)iter.next();
                    msg.append("\n").append(element.toString());
                }
            }
            
            out.println(msg.toString());
        }
    }
}