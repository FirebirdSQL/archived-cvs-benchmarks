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

import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class implements update tests from AS3AP test suite.
 */
public class UpdateTest extends BenchmarkTest {
    public UpdateTest(String name) {
        super(name);
    }
    
    protected Statement stmt;
    
	/**
	 * Set up this test case.
	 */
	protected void setUp() throws Exception {
		super.setUp();
		
		stmt = getConnection().createStatement();
	}

	/**
	 * Cleanup this test case.
	 */
	protected void tearDown() throws Exception {
		
		stmt.close();
		
		super.tearDown();
	}
    
    protected void executeUpdate(String sql) throws SQLException {
        stmt.executeUpdate(sql);
    }
    
	public void testAppendDuplicate() throws Exception {
        String sql = ""
            + "INSERT INTO " + UPDATES_TABLE + " VALUES ("
            + "6000, 0, 60000, 39997.90, 50005.00, 50005.00, "
            + "'10-nov-1985', 'CONTROLLER', 'ALICE IN WONDERLAND', "
            + "'UNIVERSITY OF ILLINOIS IN CHICAGO'"
            + ")"   
            ;
        try {
            stmt.executeUpdate(sql);
            assertTrue("Appending duplicates should fail.", false);
        } catch(SQLException ex) {
            // everything is fine
        }
	}
    
    public void testIntegrity() throws Exception {
        String sql = ""
            + "UPDATE " + HUNDRED_TABLE + " "
            + "SET " + SIGNED_COL + " = -500000000 "
            + "WHERE " + INT_COL + " = 0"
            ;
            
        try {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            // everything is ok
        }
    }
    
    public void testAppendMiddle() throws Exception {
        executeUpdate(""
            + "INSERT INTO " + UPDATES_TABLE + " VALUES ("
            + "5005, 5005, 50005, 50005.00, 50005.00, "
            + "500005.00, '1-01-1988', 'CONTROLLER', "
            + "'ALICE IN WONDERLAND', 'UNIVERSITY OF ILLINOIS IN CHICAGO'"
            + ")"
        );
    }
    
    public void testUpdateMiddle() throws Exception {
        executeUpdate(""
            + "UPDATE " + UPDATES_TABLE + " "
            + "SET " + KEY_COL + " = -5000 "
            + "WHERE " + KEY_COL + " = 5005"
        );
    }
    
    public void testDeleteMiddle() throws Exception {
        executeUpdate(""
            + "DELETE FROM " + UPDATES_TABLE + " "
            + "WHERE " + KEY_COL + " = -5000"
        );
    }
    
    public void testDropSecondaryIndices() throws Exception {
        /* @todo add test here */
    }
    public void testAppendEnd() throws Exception {
        executeUpdate(""
            + "INSERT INTO " + UPDATES_TABLE + " VALUES ("
            + "1000000001, 50005, 50005, 50005.00, 50005.00, "
            + "50005.00, '1-01-1988', 'CONTROLLER', "
            + "'ALICE IN WONDERLAND', 'UNIVERSITY OF ILLINOIS AT CHICAGO'"
            + ")"
        );
    }    
    
    public void testUpdateEnd() throws Exception {
        executeUpdate(""
            + "UPDATE " + UPDATES_TABLE + " "
            + "SET " + KEY_COL + " = -1000 "
            + "WHERE " + KEY_COL + " = 1000000001"
        );
    }
    
    public void testDeleteEnd() throws Exception {
        executeUpdate(""
            + "DELETE FROM " + UPDATES_TABLE + " "
            + "WHERE " + KEY_COL + " = -1000"
        );
    }
    
    public void testUpdateMiddleInt() throws Exception {
        executeUpdate(""
            + "UPDATE " + UPDATES_TABLE + " "
            + "SET " + INT_COL + " = 50015, " + KEY_COL + " = -5000 "
            + "WHERE " + KEY_COL + " = 5005"
        );
    }
    
    public void testUpdateMiddleCode() throws Exception {
        executeUpdate(""
            + "UPDATE " + UPDATES_TABLE + " "
            + "SET " + CODE_COL + " = 'SQL+GROUPS', " + KEY_COL + " = -5000 "
            + "WHERE " + KEY_COL + " = 5005"
        );
    }
    
    public void testBulkSave() throws Exception {
        executeUpdate(""
            + "INSERT INTO " + SAVE_UPDATES_TABLE + " "
            + "SELECT * FROM " + UPDATES_TABLE + " "
            + "WHERE " + KEY_COL + " BETWEEN 5000 AND 5999" 
        );
    }
    
    public void testBulkAppend() throws Exception {
        executeUpdate(""
            + "INSERT INTO " + UPDATES_TABLE + " "
            + "SELECT * FROM " + SAVE_UPDATES_TABLE
        );
    }
    
    public void testBulkModify() throws Exception {
        executeUpdate(""
            + "UPDATE " + UPDATES_TABLE + " "
            + "SET " + KEY_COL + " = " + KEY_COL + " - 100000 "
            + "WHERE " + KEY_COL + " BETWEEN 5000 AND 5999"
        );
    }
    
    public void testBulkDelete() throws Exception {
        executeUpdate(""
            + "DELETE FROM " + UPDATES_TABLE + " "
            + "WHERE " + KEY_COL + " < 0"
        );
    }
}