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

import java.sql.*;

/**
 * This class implements selection tests from AS3AP test suite.
 */
public class SelectTest extends BenchmarkTest {
    
    public static final String COLUMNS_FOR_SELECT = ""
        + KEY_COL + ", " + INT_COL + ", " 
        + SIGNED_COL + ", " + CODE_COL + ", "
        + DOUBLE_COL + ", " + NAME_COL
        ;
    
    public SelectTest(String name) {
        super(name);
    }

    protected Statement stmt;
    
    protected void setUp() throws Exception {
        super.setUp();
        
        stmt = getConnection().createStatement();
    }
    
    protected void tearDown() throws Exception {
        stmt.close();
        
        super.tearDown();
    }

    protected void executeSelect(String table, String condition) 
        throws SQLException 
    {
        String sql = ""
            + "SELECT " + COLUMNS_FOR_SELECT + " "
            + "FROM " + table + " "
            + "WHERE " + condition;
            
        ResultSet rs = stmt.executeQuery(sql);
                
    }
    
    public void testSelect1Clustered() throws Exception {
        executeSelect(UPDATES_TABLE, KEY_COL + " = 1000");
    }
    
    public void testSelect1NonClustered() throws Exception {
        executeSelect(UPDATES_TABLE, CODE_COL + " = 'BENCHMARKS'");
    }
    
    public void testSelect10TenPctClustered() throws Exception {
        executeSelect(TEN_PCT_TABLE, KEY_COL + " <= 10000000");
    }
    
    public void testSelect100Clustered() throws Exception {
        executeSelect(UPDATES_TABLE, KEY_COL + " <= 100");
    }

    public void testSelect100NonClustered() throws Exception {
        executeSelect(UPDATES_TABLE, INT_COL + " <= 100");
    }
    
    public void testSelect10TenPctNonClustered() throws Exception {
        executeSelect(TEN_PCT_TABLE, NAME_COL + " = 'THE+ASAP+BENCHMARK'");
    }
    
    public void testSelectVariable() throws Exception {
        
        Fetcher f = new Fetcher(new String[] {
            KEY_COL, INT_COL, SIGNED_COL, CODE_COL, DOUBLE_COL, NAME_COL
        });

        PreparedStatement ps = getConnection().prepareStatement(""
            + "SELECT " + COLUMNS_FOR_SELECT + " "
            + "FROM " + TEN_PCT_TABLE + " "
            + "WHERE " + SIGNED_COL + " < ?");

        ps.setInt(1, -500 * 1000 * 1000);
        
        ResultSet rs = ps.executeQuery();
        f.fetchResultSet(rs);
        rs.close();
        
        ps.setInt(1, -250 * 1000 * 1000);
        rs = ps.executeQuery();
        f.fetchResultSet(rs);
        rs.close();
    }
    
}