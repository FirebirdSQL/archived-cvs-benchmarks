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

import java.io.PrintStream;
import junit.framework.*;
import java.util.HashMap;
import junit.framework.TestSuite;
import junit.framework.TestResult;
import java.io.PrintWriter;
import java.util.Enumeration;

/**
 * This is modified test executor that is able to collect timings.
 */
public class TestRunner extends junit.runner.BaseTestRunner{

    public static final int SUCCESS_EXIT= 0;
    public static final int FAILURE_EXIT= 1;
    public static final int EXCEPTION_EXIT= 2;

    private TestListener testListener;
    private PrintStream writer;

    public TestRunner(TestListener listener) {
        this(System.out, listener);
    }
    
    public TestRunner(PrintStream writer, TestListener listener) {
        this.testListener = listener;
        this.writer = writer;
    }

    public void addError(Test test, Throwable throwable) {
        testListener.addError(test, throwable);
    }

    public void addFailure(Test test, AssertionFailedError assertionFailedError) {
        testListener.addFailure(test, assertionFailedError);
    }

    protected void runFailed(String string) {
        System.err.println(string);
        System.exit(-1);
    }

    private String getTestName(Test test) {
        if (test instanceof TestCase)
            return ((TestCase)test).getName();
        else
            return test.toString();
    }

    public void endTest(Test test) {
        testListener.endTest(test);
    }

    public synchronized void startTest(Test test) {
        testListener.startTest(test);
    }

    public TestResult doRun(Test suite) {
        TestResult result= new TestResult();
        result.addListener(this);
        long startTime= System.currentTimeMillis();
        suite.run(result);
        long endTime= System.currentTimeMillis();
        long runTime= endTime-startTime;
        writer.println();
        writer.println("Time: "+elapsedTimeAsString(runTime));
        print(result);

        writer.println();

        return result;
    }
    
    /**
     * Prints failures to the standard output
     */
    public synchronized void print(TestResult result) {
        printErrors(result);
        printFailures(result);
        printHeader(result);
    }
    /**
     * Prints the errors to the standard output
     */
    public void printErrors(TestResult result) {
        if (result.errorCount() != 0) {
            if (result.errorCount() == 1)
                writer.println("There was "+result.errorCount()+" error:");
            else
                writer.println("There were "+result.errorCount()+" errors:");

            int i= 1;
            for (Enumeration e= result.errors(); e.hasMoreElements(); i++) {
                TestFailure failure= (TestFailure)e.nextElement();
                writer.println(i+") "+failure.failedTest());
                writer.print(getFilteredTrace(failure.thrownException()));
            }
        }
    }
    /**
     * Prints failures to the standard output
     */
    public void printFailures(TestResult result) {
        if (result.failureCount() != 0) {
            if (result.failureCount() == 1)
                writer.println("There was " + result.failureCount() + " failure:");
            else
                writer.println("There were " + result.failureCount() + " failures:");
            int i = 1;
            for (Enumeration e= result.failures(); e.hasMoreElements(); i++) {
                TestFailure failure= (TestFailure) e.nextElement();
                writer.print(i + ") " + failure.failedTest());
                Throwable t= failure.thrownException();
                writer.print(getFilteredTrace(failure.thrownException()));
            }
        }
    }
    /**
     * Prints the header of the report
     */
    public void printHeader(TestResult result) {
        if (result.wasSuccessful()) {
            writer.println();
            writer.print("OK");
            writer.println (" (" + result.runCount() + " tests)");

        } else {
            writer.println();
            writer.println("FAILURES!!!");
            writer.println("Tests run: "+result.runCount()+ 
                         ",  Failures: "+result.failureCount()+
                         ",  Errors: "+result.errorCount());
        }
    }
    

    public static void run(Class testClass, TestListener listener) {
        new TestRunner(listener).doRun(new TestSuite(testClass));
    }

    public static void run(Test test, TestListener listener) {
        new TestRunner(listener).doRun(test);
    }
}
