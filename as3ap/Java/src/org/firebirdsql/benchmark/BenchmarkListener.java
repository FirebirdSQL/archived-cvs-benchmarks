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
import java.util.HashMap;

/**
 * This is JUnit test listener that is able to collect test execution time.
 */
public class BenchmarkListener implements TestListener {
    
    private HashMap testTimes = new HashMap();
    private HashMap startTimes = new HashMap();
    
    
    public void addError(Test test, Throwable throwable) {
        String testName = getTestName(test);
        startTimes.remove(testName);
        testTimes.remove(testName);
    }

    public void addFailure(Test test, AssertionFailedError assertionFailedError) {
        String testName = getTestName(test);
        startTimes.remove(testName);
        testTimes.remove(testName);
    }

    private String getTestName(Test test) {
        if (test instanceof TestCase)
            return ((TestCase)test).getName();
        else
            return test.toString();
    }

    public void endTest(Test test) {

        String testName = getTestName(test);
        Long start = (Long)startTimes.get(testName);

        if (start != null) {
            testTimes.put(testName, new Long(
                System.currentTimeMillis() - start.longValue()));

            startTimes.remove(testName);
        }
    }

    public synchronized void startTest(Test test) {
        String testName = getTestName(test);

        startTimes.put(testName, new Long(System.currentTimeMillis()));

    }
}