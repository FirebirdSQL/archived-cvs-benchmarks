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

import java.sql.Statement;
import java.sql.ResultSet;

/**
 * This class implements projection tests from AS3AP test suite.
 */

public class ProjectionTest extends BenchmarkTest {
    public ProjectionTest(String name) {
        super(name);
    }

    private Statement stmt;
    
    protected void setUp() throws Exception {
        super.setUp();
        
        stmt = getConnection().createStatement();
    }
    
    protected void tearDown() throws Exception {
        stmt.close();
        
        super.tearDown();
    }
    
    public void testProjection100() throws Exception {
        Fetcher f = new Fetcher(new String[] {
            ADDRESS_COL, SIGNED_COL
        });
        
        ResultSet rs = stmt.executeQuery(""
            + "SELECT DISTINCT " + ADDRESS_COL + ", " + SIGNED_COL 
            + "FROM " + HUNDRED_TABLE
        );
        
        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testProjectionTenPct() throws Exception {
        Fetcher f = new Fetcher(new String[] {
            SIGNED_COL
        });
        
        ResultSet rs = stmt.executeQuery(""
            + "SELECT DISTINCT " + SIGNED_COL + " FROM " + TEN_PCT_TABLE
        );
        
        f.fetchResultSet(rs);
        rs.close();
    }
}