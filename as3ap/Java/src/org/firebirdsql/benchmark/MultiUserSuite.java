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
import junit.framework.TestSuite;


import com.mousepushers.junit.ControllableTestThread;
import com.mousepushers.junit.MultithreadedTestCase;

/**
 * This class represents multi-user test suite from the AS3AP benchmark.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class MultiUserSuite extends MultithreadedTestCase {
    
    public static final int KEY_RANGE = 1000 * 1000 * 1000;

    public MultiUserSuite(String name) {
        super(name);
    }
    
    protected int getUserCount() {
        return 10;
    }
    
    protected TestSuite getOLTPTests(String name) {
        ActiveBenchmarkSuite suite = new ActiveBenchmarkSuite();
        
        for(int i = 0; i < getUserCount(); i++) {
            suite.addTest(new MultiUserTest("testOltpUpdate", KEY_RANGE));
        }
        
        return suite;
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
    
}
