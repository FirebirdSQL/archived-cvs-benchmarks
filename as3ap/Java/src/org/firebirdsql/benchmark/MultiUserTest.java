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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

/**
 * This class implements multiuser tests from the AS3AP test suite.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class MultiUserTest extends BenchmarkTest {
    
    public static final int DEFAULT_KEY_RANGE = 1000 * 1000 * 1000;

    
    public static final String UPDATES_KEY_COL = UPDATES_TABLE + "." + KEY_COL;
    public static final String UPDATES_DECIM_COL = UPDATES_TABLE + "." + DECIM_COL;
    public static final String UPDATES_INT_COL = UPDATES_TABLE + "." + INT_COL;
    public static final String UPDATES_DOUBLE_COL = UPDATES_TABLE + "." + DOUBLE_COL;
    
    public static final String HUNDRED_KEY_COL = HUNDRED_TABLE + "." + KEY_COL;
    
    public static final String SEL_100_SEQ_KEY_COL = SEL_100_SEQ_TABLE + "." + KEY_COL;
    public static final String SEL_100_SEQ_DOUBLE_COL = SEL_100_SEQ_TABLE + "." + DOUBLE_COL;

    public static final String SEL_100_RND_KEY_COL = SEL_100_RND_TABLE + "." + KEY_COL;
    public static final String SEL_100_RND_DOUBLE_COL = SEL_100_RND_TABLE + "." + DOUBLE_COL;

    private int keyRange;

    public MultiUserTest(String name) {
        this(name, DEFAULT_KEY_RANGE);
    }

    /**
     * Create instance of this class for the specified test case.
     * 
     * @param name name of the test case.
     * 
     * @param keyRange maximum value of the {@link KEY_COL} column, depends on
     * data size.
     */
    public MultiUserTest(String name, int keyRange) {
        super(name);
        
        this.keyRange = keyRange;
    }
    
    private Statement stmt;
    private Random rnd;
    
    protected void setUp() throws Exception {
        super.setUp();
        
        stmt = getConnection().createStatement();
        rnd = new Random();
        
        getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    }
    
    protected void tearDown() throws Exception {
        stmt.close();
        
        super.tearDown();
    }
    
    public void testOltpUpdate() throws Exception {
        stmt.executeUpdate(""
            + "UPDATE " + UPDATES_TABLE + " "
            + "SET " + SIGNED_COL + " = " + SIGNED_COL + " + 1"
            + "WHERE " + KEY_COL + " = " + rnd.nextInt(keyRange)
        );
    }
    
    public void testIrSelect() throws Exception {
        ResultSet rs = stmt.executeQuery(""
            + "SELECT " 
            + KEY_COL + ", " + CODE_COL + ", " + DATE_COL + ", "
            + SIGNED_COL + ", " + NAME_COL + " "
            + "FROM " + UPDATES_TABLE
            + "WHERE " + KEY_COL + " = " + rnd.nextInt(keyRange)
        );
        
        Fetcher f = new Fetcher(new String[]{
            KEY_COL, CODE_COL, DATE_COL, SIGNED_COL, NAME_COL
        });
        
        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testModeTiny() throws Exception {
        ResultSet rs = stmt.executeQuery(""
            + "SELECT " + KEY_COL + " "
            + "FROM " + TINY_TABLE
        );
        
        Fetcher f = new Fetcher(new String[]{
            KEY_COL
        });
        
        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testMode100k() throws Exception {
        
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        
        ResultSet rs = stmt.executeQuery(""
            + "SELECT " + COLUMNS_DEF + " "
            + "FROM " + HUNDRED_TABLE + " "
            + "WHERE " + KEY_COL + " <= 1000"
        );
        
        Fetcher f = new Fetcher(COLUMNS_ARRAY);
        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testSelect1NonClustered() throws Exception {
        String sql = ""
            + "SELECT " 
            + KEY_COL + ", " + INT_COL + ", " + SIGNED_COL + ", "
            + CODE_COL + ", " + DOUBLE_COL + ", " + NAME_COL + " "
            + "FROM " + UPDATES_TABLE + " "
            + "WHERE " + CODE_COL + "'BENCHMARKS'";
        
        Fetcher f = new Fetcher(new String[]{
            KEY_COL, INT_COL, SIGNED_COL, CODE_COL, DOUBLE_COL, NAME_COL
        });

        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        ResultSet rs = stmt.executeQuery(sql);
        f.fetchResultSet(rs);
        rs.close();
        
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        rs = stmt.executeQuery(sql);
        f.fetchResultSet(rs);
        rs.close();
        
        getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        rs = stmt.executeQuery(sql);
        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testSimpleReport() throws Exception {
        
        getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        
        ResultSet rs = stmt.executeQuery(""
            + "SELECT avg(" + UPDATES_DECIM_COL + ") "
            + "FROM " + UPDATES_TABLE + " "
            + "WHERE " + UPDATES_KEY_COL + " IN ("
            + "SELECT " + UPDATES_KEY_COL + " "
            + "FROM " + UPDATES_TABLE + ", " + HUNDRED_TABLE + " "
            + "WHERE " + HUNDRED_KEY_COL + " = " + UPDATES_KEY_COL + " "
            + "AND " + UPDATES_DECIM_COL + " > 980000000"
            + ")"
        );
        
        Fetcher f = new Fetcher(new String[]{
            UPDATES_DECIM_COL
        });
        f.fetchResultSet(rs);
        rs.close();
    }
    
    public void testSelect100Sequence() throws Exception {
        stmt.executeUpdate(""
            + "INSERT INTO " + SEL_100_SEQ_TABLE + " "
            + "SELECT * FROM " + UPDATES_TABLE + " "
            + "WHERE " + UPDATES_KEY_COL + " BETWEEN 1001 AND 1100"
        );
    }
    
    public void testSelect100Random() throws Exception {
        stmt.executeQuery(""
            + "INSERT INTO " + SEL_100_RND_TABLE + " "
            + "SELECT * FROM " + UPDATES_TABLE + " "
            + "WHERE " + UPDATES_INT_COL + " BETWEEN 1001 AND 1100"
        );
    }
    
    private void updateUpdates(String whereColumn, boolean modify) throws Exception {
        String sql = ""
            + "UPDATES " + UPDATES_TABLE + " "
            + "SET " + DOUBLE_COL + " = " + DOUBLE_COL + (modify ? "+" : "-") + " 100000000 "
            + "WHERE " + whereColumn + " BETWEEN 1001 AND 1100"
            ;   
            
        stmt.executeQuery(sql);
    }
    
    public void testModify100Sequence() throws Exception {
        
        Connection connection = getConnection();
        
        if (!connection.getAutoCommit())
            connection.setAutoCommit(false);
        
        updateUpdates(KEY_COL, true);
        
        connection.rollback();
    }
    
    public void testModify100Random() throws Exception {
        updateUpdates(INT_COL, true);
    }
    
    public void testUnmodify100Sequence() throws Exception {
        updateUpdates(KEY_COL, false);
    }
    
    public void testUnmodify100Random() throws Exception {
        updateUpdates(INT_COL, false);
    }
    
    public void testCheck100Sequence() throws Exception {
        ResultSet rs = stmt.executeQuery(""
            + "SELECT count(*) "
            + "FROM " + UPDATES_TABLE + ", " + SEL_100_SEQ_TABLE + " "
            + "WHERE " + UPDATES_KEY_COL + " = " + SEL_100_SEQ_KEY_COL + " "
            + "AND NOT " + UPDATES_DOUBLE_COL + " = " + SEL_100_SEQ_DOUBLE_COL
        );
        
        assertTrue("Should have at least one row.", rs.next());
        assertTrue("Should have no records.", rs.getInt(1) == 0);
    }
    
    public void testCheck100Random() throws Exception {
        ResultSet rs = stmt.executeQuery(""
            + "SELECT count(*) "
            + "FROM " + UPDATES_TABLE + ", " + SEL_100_RND_TABLE + " "
            + "WHERE " + UPDATES_KEY_COL + " = " + SEL_100_RND_KEY_COL + " "
            + "AND NOT " + UPDATES_DOUBLE_COL + " = " + SEL_100_RND_DOUBLE_COL
        );
        
        assertTrue("Should have at least one row.", rs.next());
        assertTrue("Should have no records.", rs.getInt(1) == 0);
    }
    
}
