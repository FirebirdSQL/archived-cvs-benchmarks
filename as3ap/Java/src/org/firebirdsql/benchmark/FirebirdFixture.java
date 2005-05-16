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

import org.firebirdsql.jdbc.FBSQLException;
import org.firebirdsql.management.FBManager;

/**
 * Fixture for Firebird database.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class FirebirdFixture extends BenchmarkFixture {
    
    /**
     * Create instance of this class for the specified benchmark database
     * manager and path to the data files.
     * 
     * @param manager instance of {@link BenchmarkDatabaseManager}.
     * @param dataPath path to the data files.
     */
    public FirebirdFixture(File dataPath) throws SQLException {
        super(dataPath);
    }
    
    /**
     * Set up the database.
     */
    public void setUp(boolean createDatabase) throws SQLException {
        
        if (createDatabase) {
            FBManager manager = 
                new FBManager(getConfig().getCustomProperty("firebird.type"));
            
            manager.setForceCreate(true);
            
            try {
                manager.start();
    
                manager.createDatabase(
                        getConfig().getCustomProperty("firebird.databasePath"), 
                        getConfig().getUserName(), 
                        getConfig().getPassword());
                
                manager.stop();
                
            } catch(Exception ex) {
                throw new FBSQLException(ex);
            }
        }
        
        super.setUp(createDatabase);
    }

    /**
     * Create instance of {@link BenchmarkDatabaseManager} that will provide
     * access to the database. 
     * 
     * @return instance of {@link BenchmarkDatabaseManager}.
     * 
     * @throws SQLException if something went wrong.
     */
    public BenchmarkDatabaseManager createDatabaseManager() throws SQLException {
        return new FirebirdDatabaseManager();
    }
}
