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

/**
 * This class contains all insert statements.
 */
public class BenchmarkInsertSQL {
    
    public static final String UNIQUES_TABLE = BenchmarkDDL.UNIQUES_TABLE;
    public static final String HUNDRED_TABLE = BenchmarkDDL.HUNDRED_TABLE;
    public static final String TEN_PCT_TABLE = BenchmarkDDL.TEN_PCT_TABLE;
    public static final String UPDATES_TABLE = BenchmarkDDL.UPDATES_TABLE;
    public static final String TINY_TABLE = BenchmarkDDL.TINY_TABLE;

    public static final String KEY_COL = BenchmarkDDL.KEY_COL;
    public static final String INT_COL = BenchmarkDDL.INT_COL;
    public static final String SIGNED_COL = BenchmarkDDL.SIGNED_COL;
    public static final String FLOAT_COL = BenchmarkDDL.FLOAT_COL;
    public static final String DOUBLE_COL = BenchmarkDDL.DOUBLE_COL;
    public static final String DECIM_COL = BenchmarkDDL.DECIM_COL;
    public static final String DATE_COL = BenchmarkDDL.DATE_COL;
    public static final String CODE_COL = BenchmarkDDL.CODE_COL;
    public static final String NAME_COL = BenchmarkDDL.NAME_COL;
    public static final String ADDRESS_COL = BenchmarkDDL.ADDRESS_COL;
    
    private static final String INSERT_COLUMNS_LIST = ""
        + BenchmarkFixture.toCSVString(new String[]{
            KEY_COL, INT_COL, SIGNED_COL, FLOAT_COL, DOUBLE_COL,
            DECIM_COL, DATE_COL, CODE_COL, NAME_COL, ADDRESS_COL
        })
        ;
    private static final String INSERT_PARAMS_LIST = ""
        + "?, ?, ?, ?, ?, "
        + "?, ?, ?, ?, ? "
        ;

    public static final String INSERT_UNIQUES = ""
        + "INSERT INTO " + UNIQUES_TABLE + "("
        + INSERT_COLUMNS_LIST
        + ") VALUES ("
        + INSERT_PARAMS_LIST
        + ")"
        ;

    public static final String INSERT_HUNDRED = ""
        + "INSERT INTO " + HUNDRED_TABLE + "("
        + INSERT_COLUMNS_LIST
        + ") VALUES ("
        + INSERT_PARAMS_LIST
        + ")"
        ;

    public static final String INSERT_TEN_PCT = ""
        + "INSERT INTO " + TEN_PCT_TABLE + "("
        + INSERT_COLUMNS_LIST
        + ") VALUES ("
        + INSERT_PARAMS_LIST
        + ")"
        ;

    public static final String INSERT_UPDATES = ""
        + "INSERT INTO " + UPDATES_TABLE + "("
        + INSERT_COLUMNS_LIST
        + ") VALUES ("
        + INSERT_PARAMS_LIST
        + ")"
        ;

    public static final String INSERT_TINY = ""
        + "INSERT INTO " + TINY_TABLE + "("
        + KEY_COL 
        + ") VALUES (" 
        + "?"
        + ")"
        ;

}