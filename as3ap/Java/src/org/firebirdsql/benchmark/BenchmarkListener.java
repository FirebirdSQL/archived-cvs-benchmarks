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
        Iterator iter = executionTrace.iterator();
        while(iter.hasNext()) {
            Test test = (Test)iter.next();
            
            String message;            
            
            Long testTime = (Long)testTimes.get(test);
            AssertionFailedError failure = 
                (AssertionFailedError)failures.get(test);
            Throwable error = (Throwable)errors.get(test);
            
            if (testTime != null)
                message = testTime.toString() + " ms";
            else
            if (failure != null)
                message = "FAILED";
            else
            if (error != null)
                message = "ERROR";
            else
                message = "UNKNOWN";
                
            out.println(getTestName(test) + " " + message);
        }
    }
}