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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

/**
 * This class implements multiuser tests from the AS3AP test suite.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public abstract class MultiUserTest extends BenchmarkTest {
    
    private static final boolean USE_PREPARED_STATEMENTS = true;
    
    /*
     * These error codes we ignore, since deadlock can easily happen during
     * concurrent execution.
     */
    public static final int DEADLOCK_ERROR_CODE = 335544451;
    public static final int DEADLOCK_ERROR_CODE2 = 335544336;
    public static final int LOCK_CONFLICT_ERROR_CODE = 335544345;
    
    
    public static final int DEFAULT_KEY_RANGE = 10 * 1000;

    
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
    
    private HashSet statements = new HashSet();
    private Random rnd;
    
    protected void setUp() throws Exception {
        super.setUp();
        
        rnd = new Random();
        
        getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    }
    
    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
    /**
     * Execute query. This method tries to hide the way statement is executed.
     * Main reason is to allow easy switch from normal statements to prepared
     * ones if underlying data source provides prepared statement pooling.
     * 
     * @param sql SQL query to execute.
     * 
     * @return instance of {@link ResultSet} corresponding to the specified
     * query.
     * 
     * @throws SQLException if something went wrong.
     */
    protected ResultSet executeQuery(String sql, Object[] params) throws SQLException {
        
        if (USE_PREPARED_STATEMENTS) {
            PreparedStatement stmt = getConnection().prepareStatement(sql);
            statements.add(stmt);
            
            setParams(stmt, params);
            
            return stmt.executeQuery();
        } else {
            Statement stmt = getConnection().createStatement();
            statements.add(stmt);
            
            return stmt.executeQuery(sql);
        }
    }

    /**
     * Execute update. This method tries to hide the way statement is executed.
     * Main reason is to allow easy switch from normal statements to prepared
     * ones if underlying data source provides prepared statement pooling.
     * 
     * @param sql SQL statement to execute.
     * 
     * @throws SQLException if something went wrong.
     */
    protected void executeUpdate(String sql, Object[] params) throws SQLException {
        try {
            if (USE_PREPARED_STATEMENTS) {
                PreparedStatement stmt = getConnection().prepareStatement(sql);
                statements.add(stmt);
                
                setParams(stmt, params);
                
                stmt.executeUpdate();
            } else {
                Statement stmt = getConnection().createStatement();
                statements.add(stmt);
                
                stmt.executeUpdate(sql);
            }
        } catch(SQLException ex) {
            
            // ignore deadlocks... 
            if (ex.getErrorCode() != DEADLOCK_ERROR_CODE &&
                ex.getErrorCode() != DEADLOCK_ERROR_CODE2 &&
                ex.getErrorCode() != LOCK_CONFLICT_ERROR_CODE
            )
                throw ex;
        }
    }    
    
    /**
     * Set parameters of prepared statement.
     * 
     * @param stmt prepared statement where parameters should be set.
     * @param params parameters of this statement.
     * 
     * @throws SQLException if something went wrong.
     */
    protected void setParams(PreparedStatement stmt, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
			stmt.setObject(i + 1, params[i]);
		}
    }
    
    /**
     * Release all statements.
     * 
     * @throws SQLException if something went wrong.
     */
    protected void releaseStatements() throws SQLException {
        Iterator iter = statements.iterator();
        while (iter.hasNext()) {
            Statement stmt = (Statement) iter.next();
            
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                // bad luck
            }
        }
        
        statements.clear();
    }
    
    protected void doOltpUpdate() throws Exception {
        try {
            executeUpdate(""
                + "UPDATE " + UPDATES_TABLE + " "
                + "SET " + SIGNED_COL + " = " + SIGNED_COL + " + 1 "
                + "WHERE " + KEY_COL + " = ?", 
                new Object[]{ new Integer(rnd.nextInt(keyRange))}
            );
        } finally {
            releaseStatements();
        }
    }
    
    protected int doIrSelect() throws Exception {
        try {
            ResultSet rs = executeQuery(""
                + "SELECT " 
                + KEY_COL + ", " + CODE_COL + ", " + DATE_COL + ", "
                + SIGNED_COL + ", " + NAME_COL + " "
                + "FROM " + UPDATES_TABLE + " "
                + "WHERE " + KEY_COL + " = ?",
                new Object[]{ new Integer(rnd.nextInt(keyRange))}
            );
            
            Fetcher f = new Fetcher(new String[]{
                KEY_COL, CODE_COL, DATE_COL, SIGNED_COL, NAME_COL
            });
            
            int result = f.fetchResultSet(rs);
            
            rs.close();
            
            return result;
        } finally {
            releaseStatements();
        }
    }
    
    protected void doModeTiny() throws Exception {
        try {
            ResultSet rs = executeQuery(""
                + "SELECT " + KEY_COL + " "
                + "FROM " + TINY_TABLE,
                new Object[0]
            );
            
            Fetcher f = new Fetcher(new String[]{
                KEY_COL
            });
            
            f.fetchResultSet(rs);
            rs.close();
        } finally {
            releaseStatements();
        }
    }
    
    protected void doMode100k() throws Exception {
        try {
            getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            
            ResultSet rs = executeQuery(""
                + "SELECT " + COLUMNS_LIST + " "
                + "FROM " + HUNDRED_TABLE + " "
                + "WHERE " + KEY_COL + " <= 1000",
                new Object[0]
            );
            
            Fetcher f = new Fetcher(COLUMNS_ARRAY);
            f.fetchResultSet(rs);
            rs.close();
        } finally {
            releaseStatements();
        }
    }
    
    protected void doSelect1NonClustered() throws Exception {
        try {
            String sql = ""
                + "SELECT " 
                + KEY_COL + ", " + INT_COL + ", " + SIGNED_COL + ", "
                + CODE_COL + ", " + DOUBLE_COL + ", " + NAME_COL + " "
                + "FROM " + UPDATES_TABLE + " "
                + "WHERE " + CODE_COL + " = 'BENCHMARKS'";
                
            Object[] params = new Object[0];
            
            Fetcher f = new Fetcher(new String[]{
                KEY_COL, INT_COL, SIGNED_COL, CODE_COL, DOUBLE_COL, NAME_COL
            });
            
            // workaround for bad drivers like InterClient
            if (getConnection().getAutoCommit())
                getConnection().commit();
    
            getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            ResultSet rs = executeQuery(sql, params);
            f.fetchResultSet(rs);
            rs.close();

            // workaround for bad drivers like InterClient
            if (getConnection().getAutoCommit())
                getConnection().commit();
            
            getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            rs = executeQuery(sql, params);
            f.fetchResultSet(rs);
            rs.close();

            // workaround for bad drivers like InterClient
            if (getConnection().getAutoCommit())
                getConnection().commit();
            
            getConnection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            rs = executeQuery(sql, params);
            f.fetchResultSet(rs);
            rs.close();
        } finally {
            releaseStatements();
        }
    }
    
    protected void doSimpleReport() throws Exception {
        try {
            getConnection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            
            ResultSet rs = executeQuery(""
                + "SELECT avg(" + UPDATES_DECIM_COL + ") "
                + "FROM " + UPDATES_TABLE + " "
                + "WHERE " + UPDATES_KEY_COL + " IN ("
                + "SELECT " + UPDATES_KEY_COL + " "
                + "FROM " + UPDATES_TABLE + ", " + HUNDRED_TABLE + " "
                + "WHERE " + HUNDRED_KEY_COL + " = " + UPDATES_KEY_COL + " "
                + "AND " + UPDATES_DECIM_COL + " > 980000000"
                + ")",
                new Object[0]
            );
            
            Fetcher f = new Fetcher(new String[]{
                UPDATES_DECIM_COL
            });
            f.fetchResultSet(rs);
            rs.close();
        } finally {
            releaseStatements();
        }
    }
    
    protected void doSelect100Sequence() throws Exception {
        try {
            executeUpdate(""
                + "INSERT INTO " + SEL_100_SEQ_TABLE + " "
                + "SELECT * FROM " + UPDATES_TABLE + " "
                + "WHERE " + UPDATES_KEY_COL + " BETWEEN 1001 AND 1100",
                new Object[0]
            );
        } finally {
            releaseStatements();
        }
    }
    
    protected void doSelect100Random() throws Exception {
        try {
            executeUpdate(""
                + "INSERT INTO " + SEL_100_RND_TABLE + " "
                + "SELECT * FROM " + UPDATES_TABLE + " "
                + "WHERE " + UPDATES_INT_COL + " BETWEEN 1001 AND 1100",
                new Object[0]
            );
        } finally {
            releaseStatements();
        }
    }
    
    private void updateUpdates(String whereColumn, boolean modify) throws Exception {
        try {
            String sql = ""
                + "UPDATE " + UPDATES_TABLE + " "
                + "SET " + DOUBLE_COL + " = " + DOUBLE_COL + (modify ? "+" : "-") + " 100000000 "
                + "WHERE " + whereColumn + " BETWEEN 1001 AND 1100"
                ;   
                
            executeUpdate(sql, new Object[0]);
        } finally {
            releaseStatements();
        }
    }
    
    protected void doModify100Sequence() throws Exception {
        
        Connection connection = getConnection();
        
        if (!connection.getAutoCommit())
            connection.setAutoCommit(false);
        
        updateUpdates(KEY_COL, true);
        
        connection.rollback();
    }
    
    protected void doModify100Random() throws Exception {
        updateUpdates(INT_COL, true);
    }
    
    protected void doUnmodify100Sequence() throws Exception {
        updateUpdates(KEY_COL, false);
    }
    
    protected void doUnmodify100Random() throws Exception {
        updateUpdates(INT_COL, false);
    }
    
    protected void doCheck100Sequence() throws Exception {
        try {
            ResultSet rs = executeQuery(""
                + "SELECT count(*) "
                + "FROM " + UPDATES_TABLE + ", " + SEL_100_SEQ_TABLE + " "
                + "WHERE " + UPDATES_KEY_COL + " = " + SEL_100_SEQ_KEY_COL + " "
                + "AND NOT " + UPDATES_DOUBLE_COL + " = " + SEL_100_SEQ_DOUBLE_COL,
                new Object[0]
            );
            
            assertTrue("Should have at least one row.", rs.next());
            
            int rowCount = rs.getInt(1);
            assertTrue(
                "Should have no records, but reported " + rowCount + ".", 
                rowCount == 0);
        } finally {
            releaseStatements();
        }
    }
    
    protected void doCheck100Random() throws Exception {
        try {
            ResultSet rs = executeQuery(""
                + "SELECT count(*) "
                + "FROM " + UPDATES_TABLE + ", " + SEL_100_RND_TABLE + " "
                + "WHERE " + UPDATES_KEY_COL + " = " + SEL_100_RND_KEY_COL + " "
                + "AND NOT " + UPDATES_DOUBLE_COL + " = " + SEL_100_RND_DOUBLE_COL,
                new Object[0]
            );
            
            assertTrue("Should have at least one row.", rs.next());
            
            int rowCount = rs.getInt(1);
            assertTrue(
                "Should have no records, but reported " + rowCount + ".", 
                rowCount == 0);
        } finally {
            releaseStatements();
        }
    }
    
}
