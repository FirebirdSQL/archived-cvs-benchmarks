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

import java.io.File;
import java.sql.SQLException;


/**
 * Standard JDBC driver fixture. 
 */
public class JdbcFixture extends BenchmarkFixture {

    /**
     * Create instance of this class for the specified path.
     * 
     * @param dataPath path to the files with data.
     * 
     * @throws SQLException if something went wrong.
     */
    public JdbcFixture(File dataPath) throws SQLException {
        super(dataPath);
    }

    /**
     * Create the databse manager.
     * 
     * @return instance of {@link JdbcDatabaseManager}
     */
    public BenchmarkDatabaseManager createDatabaseManager() throws SQLException {
        return new JdbcDatabaseManager();
    }

}
