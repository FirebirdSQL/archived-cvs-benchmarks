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
 * Background test cases. Each tests executes corresponding statement in an
 * endless loop until it is stopped. There's small delay between subsequent
 * test executions to allow better scheduling.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class BackgroundMultiUserTest extends MultiUserTest {
    
    public BackgroundMultiUserTest(String name, int keyRange) {
        super(name, keyRange);
    }

    protected void setUp() throws Exception {
        super.setUp();
        
        stopped = false;
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
    private boolean stopped;
    
    public synchronized void stop() {
        stopped = true;
        notifyAll();
    }
        
    public void testOltpUpdate() throws Exception {
        while(!stopped) {
            doOltpUpdate();
            
            int sleepDuration = getDatabaseManager().getConfig().getSleepDuration();
            if (sleepDuration > 0)
                synchronized(this) {
                    wait(sleepDuration);
                }
        }
        
        System.out.println("[" + Thread.currentThread().getName() + "] stopped.");
    }

    public void testIrSelect() throws Exception {
        while(!stopped) {
            doIrSelect();
            
            int sleepDuration = getDatabaseManager().getConfig().getSleepDuration();
            if (sleepDuration > 0)
                synchronized(this) {
                    wait(sleepDuration);
                }
        }
        
        System.out.println("[" + Thread.currentThread().getName() + "] stopped.");
    }
}