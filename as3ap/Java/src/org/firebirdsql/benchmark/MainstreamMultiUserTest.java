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

/**
 * Mainstream tests. Information retrieval test is executed during specified
 * duration, other tests are simply execute only once.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class MainstreamMultiUserTest extends MultiUserTest {
    
    private int duration;
    
    public MainstreamMultiUserTest(String name, int keyRange, int duration) {
        super(name, keyRange);
        
        this.duration = duration;
    }

    private int throughput;
    
    public int getThroughput() {
        return throughput;
    }
    
    public synchronized void testIrSelect() throws Exception {
        long start = System.currentTimeMillis();
        
        while(System.currentTimeMillis() - start < duration) {
            throughput += doIrSelect();
            
            int sleepDuration = getDatabaseManager().getConfig().getSleepDuration();
            if (sleepDuration > 0)
                wait(sleepDuration);
        }
    }
    
    public void testCrossSection() throws Exception {
        doModeTiny();
        doMode100k();
        doSelect1NonClustered();
        doSimpleReport();
        doSelect100Sequence();
        doSelect100Random();
        doModify100Sequence();
        doModify100Random();
        doUnmodify100Sequence();
        doUnmodify100Random();
    }
    
    public void testCheck() throws Exception {
        doCheck100Sequence();
        doCheck100Random();
    }
}