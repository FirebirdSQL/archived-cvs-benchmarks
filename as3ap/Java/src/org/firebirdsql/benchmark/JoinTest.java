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
 * This class implements join tests from the AS3AP test suite.
 */
public class JoinTest extends BenchmarkTest {
    
    public static final String UNIQUES_KEY_COL = UNIQUES_TABLE + "." + KEY_COL;
    public static final String UNIQUES_SIGNED_COL = UNIQUES_TABLE + "." + SIGNED_COL;
    public static final String UNIQUES_NAME_COL = UNIQUES_TABLE + "." + NAME_COL;
    public static final String UNIQUES_DATE_COL = UNIQUES_TABLE + "." + DATE_COL;
    public static final String UNIQUES_CODE_COL = UNIQUES_TABLE + "." + CODE_COL;
    public static final String UNIQUES_ADDRESS_COL = UNIQUES_TABLE + "." + ADDRESS_COL;
    
    public static final String UPDATES_KEY_COL = UPDATES_TABLE + "." + KEY_COL;
    public static final String UPDATES_SIGNED_COL = UPDATES_TABLE + "." + SIGNED_COL;
    public static final String UPDATES_NAME_COL = UPDATES_TABLE + "." + NAME_COL;
    public static final String UPDATES_DATE_COL = UPDATES_TABLE + "." + DATE_COL;
    public static final String UPDATES_CODE_COL = UPDATES_TABLE + "." + CODE_COL;

    public static final String TEN_PCT_KEY_COL = TEN_PCT_TABLE + "." + KEY_COL;
    public static final String TEN_PCT_SIGNED_COL = TEN_PCT_TABLE + "." + SIGNED_COL;
    public static final String TEN_PCT_NAME_COL = TEN_PCT_TABLE + "." + NAME_COL;
    public static final String TEN_PCT_DATE_COL = TEN_PCT_TABLE + "." + DATE_COL;
    public static final String TEN_PCT_CODE_COL = TEN_PCT_TABLE + "." + CODE_COL;

    public static final String HUNDRED_KEY_COL = HUNDRED_TABLE + "." + KEY_COL;
    public static final String HUNDRED_SIGNED_COL = HUNDRED_TABLE + "." + SIGNED_COL;
    public static final String HUNDRED_NAME_COL = HUNDRED_TABLE + "." + NAME_COL;
    public static final String HUNDRED_DATE_COL = HUNDRED_TABLE + "." + DATE_COL;
    public static final String HUNDRED_CODE_COL = HUNDRED_TABLE + "." + CODE_COL;
    public static final String HUNDRED_ADDRESS_COL = HUNDRED_TABLE + "." + ADDRESS_COL;
    
    
    public JoinTest(String name) {
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
    
    public void testJoin2Clustered() throws Exception {

        Fetcher f = new Fetcher(new String[] {
            UNIQUES_SIGNED_COL, UNIQUES_NAME_COL,
            HUNDRED_SIGNED_COL, HUNDRED_NAME_COL
        });

        ResultSet rs = stmt.executeQuery(""
            + "SELECT " 
            + UNIQUES_SIGNED_COL + ", " + UNIQUES_NAME_COL + ", "
            + HUNDRED_SIGNED_COL + ", " + HUNDRED_NAME_COL + " "
            + "FROM " + UNIQUES_TABLE + ", " + HUNDRED_TABLE + " "
            + "WHERE "
            + UNIQUES_KEY_COL + " = " + HUNDRED_KEY_COL + " "
            + "AND "
            + UNIQUES_KEY_COL + " = 1000"
        );

        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testJoin2NonClustered() throws Exception {
        Fetcher f = new Fetcher(new String[] {
            UNIQUES_SIGNED_COL, UNIQUES_NAME_COL,
            HUNDRED_SIGNED_COL, HUNDRED_NAME_COL
        });

        ResultSet rs = stmt.executeQuery(""
            + "SELECT " 
            + UNIQUES_SIGNED_COL + ", " + UNIQUES_NAME_COL + ", "
            + HUNDRED_SIGNED_COL + ", " + HUNDRED_NAME_COL + " "
            + "FROM " + UNIQUES_TABLE + ", " + HUNDRED_TABLE + " "
            + "WHERE "
            + UNIQUES_CODE_COL + " = " + HUNDRED_CODE_COL + " "
            + "AND "
            + UNIQUES_CODE_COL + " = 'BENCHMARKS'"
        );

        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testJoin2() throws Exception {
        Fetcher f = new Fetcher(new String[] {
            UNIQUES_SIGNED_COL, UNIQUES_NAME_COL,
            HUNDRED_SIGNED_COL, HUNDRED_NAME_COL
        });

        ResultSet rs = stmt.executeQuery(""
            + "SELECT " 
            + UNIQUES_SIGNED_COL + ", " + UNIQUES_NAME_COL + ", "
            + HUNDRED_SIGNED_COL + ", " + HUNDRED_NAME_COL + " "
            + "FROM " + UNIQUES_TABLE + ", " + HUNDRED_TABLE + " "
            + "WHERE "
            + UNIQUES_ADDRESS_COL + " = " + HUNDRED_ADDRESS_COL + " "
            + "AND "
            + UNIQUES_ADDRESS_COL + " = 'SILICON VALLEY'"
        );

        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testJoin3Clustered() throws Exception {
        Fetcher f = new Fetcher(new String[] {
            UNIQUES_SIGNED_COL, UNIQUES_DATE_COL,
            HUNDRED_SIGNED_COL, HUNDRED_DATE_COL,
            TEN_PCT_SIGNED_COL, TEN_PCT_DATE_COL
        });
        
        ResultSet rs = stmt.executeQuery(""
            + "SELECT " 
            + UNIQUES_SIGNED_COL + ", " + UNIQUES_NAME_COL + ", "
            + HUNDRED_SIGNED_COL + ", " + HUNDRED_NAME_COL + ", "
            + TEN_PCT_SIGNED_COL + ", " + TEN_PCT_NAME_COL + " "
            + "FROM " + UNIQUES_TABLE + ", " 
            + HUNDRED_TABLE + ", " + TEN_PCT_TABLE + " "
            + "WHERE "
            + UNIQUES_KEY_COL + " = " + HUNDRED_KEY_COL + " "
            + "AND "
            + UNIQUES_KEY_COL + " = " + TEN_PCT_KEY_COL + " "
            + "AND "
            + UNIQUES_KEY_COL + " = 1000"
        );

        f.fetchResultSet(rs);
        rs.close();
    }

    public void testJoin3NonClustered() throws Exception {
        Fetcher f = new Fetcher(new String[] {
            UNIQUES_SIGNED_COL, UNIQUES_DATE_COL,
            HUNDRED_SIGNED_COL, HUNDRED_DATE_COL,
            TEN_PCT_SIGNED_COL, TEN_PCT_DATE_COL
        });

        ResultSet rs = stmt.executeQuery(""
            + "SELECT " 
            + UNIQUES_SIGNED_COL + ", " + UNIQUES_NAME_COL + ", "
            + HUNDRED_SIGNED_COL + ", " + HUNDRED_NAME_COL + ", "
            + TEN_PCT_SIGNED_COL + ", " + TEN_PCT_NAME_COL + " "
            + "FROM " + UNIQUES_TABLE + ", " 
            + HUNDRED_TABLE + ", " + TEN_PCT_TABLE + " "
            + "WHERE "
            + UNIQUES_CODE_COL + " = " + HUNDRED_CODE_COL + " "
            + "AND "
            + UNIQUES_CODE_COL + " = " + TEN_PCT_CODE_COL + " "
            + "AND "
            + UNIQUES_CODE_COL + " = 'BENCHMARKS'"
        );

        f.fetchResultSet(rs);
        rs.close();
    }

    public void testJoin4Clustered() throws Exception {
        Fetcher f = new Fetcher(new String[] {
            UNIQUES_SIGNED_COL, UNIQUES_DATE_COL,
            HUNDRED_SIGNED_COL, HUNDRED_DATE_COL,
            TEN_PCT_SIGNED_COL, TEN_PCT_DATE_COL
        });

        ResultSet rs = stmt.executeQuery(""
            + "SELECT " 
            + UNIQUES_DATE_COL + ", " + HUNDRED_DATE_COL + ", "
            + TEN_PCT_DATE_COL + ", " + UPDATES_DATE_COL + " "
            + "FROM " + UNIQUES_TABLE + ", " + HUNDRED_TABLE + ", " 
            + TEN_PCT_TABLE + ", " + UPDATES_TABLE + " "
            + "WHERE "
            + UNIQUES_KEY_COL + " = " + HUNDRED_KEY_COL + " "
            + "AND "
            + UNIQUES_KEY_COL + " = " + TEN_PCT_KEY_COL + " "
            + "AND "
            + UNIQUES_KEY_COL + " = " + UPDATES_KEY_COL + " "
            + "AND "
            + UNIQUES_KEY_COL + " = 1000"
        );

        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testJoin4NonClustered() throws Exception {
        Fetcher f = new Fetcher(new String[] {
            UNIQUES_DATE_COL, HUNDRED_DATE_COL,
            TEN_PCT_DATE_COL, UPDATES_DATE_COL
        });

        ResultSet rs = stmt.executeQuery(""
            + "SELECT " 
            + UNIQUES_DATE_COL + ", " + HUNDRED_DATE_COL + ", "
            + TEN_PCT_DATE_COL + ", " + UPDATES_DATE_COL + " "
            + "FROM " + UNIQUES_TABLE + ", " + HUNDRED_TABLE + ", " 
            + TEN_PCT_TABLE + ", " + UPDATES_TABLE + " "
            + "WHERE "
            + UNIQUES_CODE_COL + " = " + HUNDRED_CODE_COL + " "
            + "AND "
            + UNIQUES_CODE_COL + " = " + TEN_PCT_CODE_COL + " "
            + "AND "
            + UNIQUES_CODE_COL + " = " + UPDATES_CODE_COL + " "
            + "AND "
            + UNIQUES_CODE_COL + " = 'BENCHMARKS'"
        );

        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testJoin110() throws Exception {
        Fetcher f = new Fetcher(new String[] {
            UNIQUES_KEY_COL, UNIQUES_NAME_COL,
            TEN_PCT_NAME_COL, TEN_PCT_SIGNED_COL
        });
        
        ResultSet rs = stmt.executeQuery(""
            + "SELECT "
            + UNIQUES_KEY_COL + ", " + UNIQUES_NAME_COL + ", "
            + TEN_PCT_NAME_COL + ", " + TEN_PCT_SIGNED_COL + " "
            + "FROM " + UNIQUES_TABLE + ", " + TEN_PCT_TABLE + " "
            + "WHERE " 
            + UNIQUES_KEY_COL + " = " + TEN_PCT_KEY_COL + " "
            + "AND "
            + UNIQUES_KEY_COL + 
            " IN (500000000, 600000000, 700000000, 800000000, 900000000)"
        );
        
        f.fetchResultSet(rs);
        rs.close();
    }
}