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

/**
 * This class contains all index tests
 */
public class IndexTest extends BenchmarkTest {
    
    public static final String TEN_PCT_KEY_CODE_IDX = "\"ten_pct_key_code\"";
    public static final String TEN_PCT_INT_IDX = "\"ten_pct_int\"";
    public static final String TEN_PCT_SIGNED_IDX = "\ten_pct_signed\"";
    public static final String TEN_PCT_DOUBLE_IDX = "\"ten_pct_double\"";
    public static final String TEN_PCT_FLOAT_IDX = "\"ten_pct_float\"";
    public static final String TEN_PCT_DECIM_IDX = "\"ten_pct_decim\"";
    public static final String TEN_PCT_NAME_IDX = "\"ten_pct_name\"";
    public static final String TEN_PCT_CODE_IDX = "\"ten_pct_code\"";
    
    public static final String UNIQUES_CODE_IDX = "\"uniques_code\"";

    public static final String UPDATES_DECIM_IDX = "\"updates_decim\"";
    public static final String UPDATES_INT_IDX = "\"updates_int\"";
    public static final String UPDATES_CODE_IDX = "\"updates_code\"";
    public static final String UPDATES_DOUBLE_IDX = "\"updates_double\"";
    
    public static final String HUNDRED_CODE_IDX = "\"hundred_code\"";
    
    public IndexTest(String string) {
        super(string);
    }

    private void createIndex(String indexName, String tableName, 
        String[] keys, boolean unique) throws SQLException
    {
        
        String indexType = unique ? "UNIQUE " : "";
            
        String sql = "CREATE " + indexType +"INDEX " + indexName + 
            " ON " + tableName + "(" + BenchmarkFixture.toCSVString(keys) + ")";
            
        getDatabaseManager().executeDDL(sql);
    }
    
    private void dropIndex(String indexName) throws SQLException {
        String sql = "DROP INDEX " + indexName;
        
        getDatabaseManager().executeDDL(sql);
    }
    
    // check if this is needed
    public void testTenPctKeyCodeIndex() throws Exception {
        createIndex(TEN_PCT_KEY_CODE_IDX, TEN_PCT_TABLE, 
            new String[]{KEY_COL, CODE_COL}, true);
    }

    public void testTenPctIntIndex() throws Exception {
        createIndex(TEN_PCT_INT_IDX, TEN_PCT_TABLE, 
            new String[] {INT_COL}, false);
    }
    
    public void testTenPctSignedIndex() throws Exception {
        createIndex(TEN_PCT_SIGNED_IDX, TEN_PCT_TABLE, 
            new String[] {SIGNED_COL}, false);
    }
    
    public void testUniquesCodeIndex() throws Exception {
        createIndex(UNIQUES_CODE_IDX, UNIQUES_TABLE, 
            new String[] {CODE_COL}, false);
    }
    
    public void testTenPctDoubleIndex() throws Exception {
        createIndex(TEN_PCT_DOUBLE_IDX, TEN_PCT_TABLE,
            new String[] {DOUBLE_COL}, false);
    }
    
    public void testUpdatesDecimIndex() throws Exception {
        createIndex(UPDATES_DECIM_IDX, UPDATES_TABLE,
            new String[] {DECIM_COL}, false);
    }
    
    public void testTenPctFloatIndex() throws Exception {
        createIndex(TEN_PCT_FLOAT_IDX, TEN_PCT_TABLE, 
            new String[] {FLOAT_COL}, false);
    }
    
    public void testUpdatesIntIndex() throws Exception {
        createIndex(UPDATES_INT_IDX, UPDATES_TABLE,
            new String[] {INT_COL}, false);
    }
    
    public void testTenPctDecimIndex() throws Exception {
        createIndex(TEN_PCT_DECIM_IDX, TEN_PCT_TABLE,
            new String[] {DECIM_COL}, false);
    }
    
    public void testHundredCodeIndex() throws Exception {
        createIndex(HUNDRED_CODE_IDX, HUNDRED_TABLE,
            new String[] {CODE_COL}, false);
    }
    
    public void testTenPctNameIndex() throws Exception {
        createIndex(TEN_PCT_NAME_IDX, TEN_PCT_TABLE,
            new String[] {NAME_COL}, false);
    }
    
    public void testUpdatesCodeIndex() throws Exception {
        createIndex(UPDATES_CODE_IDX, UPDATES_TABLE,
            new String[] {CODE_COL}, false);
    }
    
    public void testTenPctCodeIndex() throws Exception {
        createIndex(TEN_PCT_CODE_IDX, TEN_PCT_TABLE,
            new String[] {CODE_COL}, false);
    }
    
    public void testUpdatesDoubleIndex() throws Exception {
        createIndex(UPDATES_DOUBLE_IDX, UPDATES_TABLE,
            new String[] {DOUBLE_COL}, false);
    }
    
    public void testUpdatesDropIndices() throws Exception {
        dropIndex(UPDATES_CODE_IDX);
        dropIndex(UPDATES_DECIM_IDX);
        dropIndex(UPDATES_DOUBLE_IDX);
        dropIndex(UPDATES_INT_IDX);
    }
}