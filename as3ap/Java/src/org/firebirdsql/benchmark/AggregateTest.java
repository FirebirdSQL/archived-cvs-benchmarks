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
import java.sql.SQLException;

/**
 * This class implements aggregate tests from AS3AP test suite.
 */
public class AggregateTest extends BenchmarkTest {
    
    public static final String UPDATES_KEY_COL = UPDATES_TABLE + "." + KEY_COL;
    public static final String UPDATES_SIGNED_COL = UPDATES_TABLE + "." + SIGNED_COL;
    public static final String UPDATES_DECIM_COL = UPDATES_TABLE + "." + DECIM_COL;
    public static final String UPDATES_DATE_COL = UPDATES_TABLE + "." + DATE_COL;

    public static final String HUNDRED_NAME_COL = HUNDRED_TABLE + "." + NAME_COL;
    public static final String HUNDRED_KEY_COL = HUNDRED_TABLE + "." + KEY_COL;
    public static final String HUNDRED_CODE_COL = HUNDRED_TABLE + "." + CODE_COL;
    public static final String HUNDRED_INT_COL = HUNDRED_TABLE + "." + INT_COL;
    
    public AggregateTest(String name) {
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
    
    private void fetchResultSet(ResultSet rs) throws SQLException {
        while(rs.next()) {
            Object obj = rs.getObject(1);
        }
    }
    
    public void testScalarAggregate() throws Exception {
        ResultSet rs = stmt.executeQuery(
            "SELECT min(" + KEY_COL + ") FROM " + UNIQUES_TABLE);
            
        fetchResultSet(rs);
        rs.close();
    }
    
    public void testFunctionAggregate() throws Exception {
        ResultSet rs = stmt.executeQuery(""
            + "SELECT min(" + KEY_COL + ") FROM " + HUNDRED_TABLE + " "
            + "GROUP BY " + NAME_COL
        );
        
        fetchResultSet(rs);
        rs.close();
    }
    
    public void testInfoRetrieval() throws Exception {
        ResultSet rs = stmt.executeQuery(""
            + "SELECT count(" + KEY_COL + ") "
            + "FROM " + TEN_PCT_TABLE + " "
            + "WHERE " 
            + NAME_COL + " = 'THE+ASAP+BENCHMARKS+' "
            + "AND "
            + INT_COL + " <= 100000000 " 
            + "AND " 
            + SIGNED_COL + " BETWEEN 1 AND 99999999 "
            + "AND "
            + "NOT(" + FLOAT_COL + " BETWEEN -450000000 AND 450000000) "
            + "AND "
            + DOUBLE_COL + " > 600000000 " 
            + "AND " 
            + DECIM_COL + " < -600000000"
        );
        
        fetchResultSet(rs);
        rs.close();
    }
    
    public void testSimpleReport() throws Exception {
        ResultSet rs = stmt.executeQuery(""
            + "SELECT avg(" + DECIM_COL + ") "
            + "FROM " + UPDATES_TABLE
            + "WHERE "
            + KEY_COL + " IN ("
            + "SELECT " + UPDATES_TABLE + "." + KEY_COL 
            + "FROM " + UPDATES_TABLE + ", " + HUNDRED_TABLE + " "
            + "WHERE " 
            + HUNDRED_TABLE + "." + KEY_COL + " = " 
            + UPDATES_TABLE + "." + KEY_COL + " "
            + "AND " + UPDATES_TABLE + "." + DECIM_COL + " > 980000000"
            + ")"
        );
        
        fetchResultSet(rs);
        rs.close();
    }
    
    public void testCreateView() throws Exception {
        String sql = "CREATE VIEW reportview(" 
            + KEY_COL + ", " + SIGNED_COL + ", "
            + DATE_COL + ", " + DECIM_COL + ", "
            + NAME_COL + ", " + CODE_COL + ", "
            + INT_COL + ") "
            + "AS "
            + "SELECT "
            + UPDATES_KEY_COL + ", " + UPDATES_SIGNED_COL + ", "
            + UPDATES_DATE_COL + ", " + UPDATES_DECIM_COL + ", "
            + HUNDRED_NAME_COL + ", " + HUNDRED_CODE_COL + ", "
            + HUNDRED_INT_COL + " "
            + "FROM " + UPDATES_TABLE + ", " + HUNDRED_TABLE
            + "WHERE " + UPDATES_KEY_COL + " = " + HUNDRED_KEY_COL
            ;

        getDatabaseManager().executeUpdate(sql);
    }
    
    public void testSubtotalReport() throws Exception {
        String sql = ""
            + "SELECT "
            + "avg(" + SIGNED_COL + "), "
            + "min(" + SIGNED_COL + "), "
            + "max(" + SIGNED_COL + "), "
            + "max(" + DATE_COL + "), "
            + "min(" + DATE_COL + "), "
            + "count(DISTINCT " + NAME_COL + "), "
            + "count(" + NAME_COL + "), "
            + CODE_COL + ", "
            + INT_COL + " "
            + "FROM reportview "
            + "WHERE " + DECIM_COL + " > 980000000 "
            + "GROUP BY " + CODE_COL + ", " + INT_COL
            ;
    }
    
    public void testTotalReport() throws Exception {
        String sql = ""
            + "SELECT "
            + "avg(" + SIGNED_COL + "), "
            + "min(" + SIGNED_COL + "), "
            + "max(" + SIGNED_COL + "), "
            + "max(" + DATE_COL + "), "
            + "min(" + DATE_COL + "), "
            + "count(DISTINCT " + NAME_COL + "), "
            + "count(" + NAME_COL + ") "
            + "count(" + CODE_COL + "), "
            + "count(" + INT_COL + ") "
            + "FROM reportview "
            + "WHERE " + DECIM_COL + " > 980000000 "
            ;
    }
    
}