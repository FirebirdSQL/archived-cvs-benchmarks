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

import junit.framework.TestCase;

/**
 * This class contains DDL SQL statements and is base class for the benchmark
 * tests.
 */

public abstract class BenchmarkDDL extends TestCase {
    
    public static final String UNIQUES_TABLE = "\"uniques\"";
    public static final String HUNDRED_TABLE = "\"hundred\"";
    public static final String TEN_PCT_TABLE = "\"tenpct\"";
    public static final String UPDATES_TABLE = "\"updates\"";
    public static final String TINY_TABLE = "\"tiny\"";
    
    public static final String SAVE_UPDATES_TABLE = "\"saveupdates\"";
    public static final String SEL_100_SEQ_TABLE = "\"sel100seq\"";
    public static final String SEL_100_RND_TABLE = "\"sel100rnd\"";
    
    public static final String UNIQUES_TABLE_OUTPUT = "\"uniques_output\"";
    public static final String HUNDRED_TABLE_OUTPUT = "\"hundred_output\"";
    public static final String TEN_PCT_TABLE_OUTPUT = "\"tenpct_output\"";
    public static final String UPDATES_TABLE_OUTPUT = "\"updates_output\"";
    public static final String TINY_TABLE_OUTPUT = "\"tiny_output\"";

    public static final String KEY_COL = "\"key\"";
    public static final String INT_COL = "\"int\"";
    public static final String SIGNED_COL = "\"signed\"";
    public static final String FLOAT_COL = "\"float\"";
    public static final String DOUBLE_COL = "\"double\"";
    public static final String DECIM_COL = "\"decim\"";
    public static final String DATE_COL = "\"date\"";
    public static final String CODE_COL = "\"code\"";
    public static final String NAME_COL = "\"name\"";
    public static final String ADDRESS_COL = "\"address\"";
    
    public static final String COLUMNS_DEF = ""
        + KEY_COL + " INTEGER NOT NULL, "
        + INT_COL + " INTEGER NOT NULL, "
        + SIGNED_COL + " INTEGER, "
        + FLOAT_COL + " FLOAT NOT NULL, "
        + DOUBLE_COL + " DOUBLE PRECISION NOT NULL, "
        + DECIM_COL + " NUMERIC(18,2) NOT NULL, "
        + DATE_COL + " TIMESTAMP NOT NULL, "
        + CODE_COL + " CHAR(10) NOT NULL, "
        + NAME_COL + " CHAR(20) NOT NULL, "
        + ADDRESS_COL + " VARCHAR(80) NOT NULL "
        ;
        
    public static final String[] COLUMNS_ARRAY = new String[] {
        KEY_COL, INT_COL, SIGNED_COL, FLOAT_COL, DOUBLE_COL,
        DECIM_COL, DATE_COL, CODE_COL, NAME_COL, ADDRESS_COL
    };
    
    public static final String CREATE_UNIQUES_TABLE = ""
        + "CREATE TABLE " + UNIQUES_TABLE + "("
        + COLUMNS_DEF + ", "
        + "PRIMARY KEY ("+ KEY_COL + ")"
        + ")"
        ;
        
    public static final String CREATE_HUNDRED_TABLE = ""
        + "CREATE TABLE " + HUNDRED_TABLE + "("
        + COLUMNS_DEF + ", "
        + "PRIMARY KEY (" + KEY_COL + "), "
        + "FOREIGN KEY (" + SIGNED_COL + ") REFERENCES " + UPDATES_TABLE
        + ")"
        ;
        
    public static final String CREATE_TEN_PCT_TABLE = ""
        + "CREATE TABLE " + TEN_PCT_TABLE + "("
        + COLUMNS_DEF + ", "
        + "PRIMARY KEY (" + KEY_COL + ", " + CODE_COL + ")"
        + ")"
        ;
        
    public static final String CREATE_UPDATES_TABLE = ""
        + "CREATE TABLE " + UPDATES_TABLE + "("
        + COLUMNS_DEF + ", "
        + "PRIMARY KEY (" + KEY_COL + ")"
        + ")"
        ;
        
    public static final String CREATE_TINY_TABLE = ""
        + "CREATE TABLE " + TINY_TABLE + "("
        + KEY_COL + " INTEGER NOT NULL, " 
        + "PRIMARY KEY (" + KEY_COL + ")"
        + ")"
        ;
        
        
    public static final String CREATE_TINY_OUTPUT = ""
        + "CREATE TABLE " + TINY_TABLE_OUTPUT + "("
        + KEY_COL + " INTEGER NOT NULL"
        + ")"
        ;

    public static final String CREATE_UPDATES_OUTPUT = ""
        + "CREATE TABLE "+ UPDATES_TABLE_OUTPUT +"("
        + COLUMNS_DEF
        + ")"
        ;

    public static final String CREATE_HUNDRED_OUTPUT = ""
        + "CREATE TABLE " + HUNDRED_TABLE_OUTPUT + "("
        + COLUMNS_DEF
        + ")"
        ;
        
    public static final String CREATE_SAVE_UPDATES_TABLE = ""
        + "CREATE TABLE " + SAVE_UPDATES_TABLE + "("
        + COLUMNS_DEF
        + ")"
        ;
        
    public static final String CREATE_SEL_100_SEQ_TABLE = ""
        + "CREATE TABLE " + SEL_100_SEQ_TABLE + "("
        + COLUMNS_DEF + ", "
        + "PRIMARY KEY (" + KEY_COL + ")"
        + ")"
        ;
        
    public static final String CREATE_SEL_100_RND_TABLE = ""
        + "CREATE TABLE " + SEL_100_RND_TABLE + "("
        + COLUMNS_DEF + ", "
        + "PRIMARY KEY (" + KEY_COL + ")"
        + ")"
        ;
        
    public static final String DROP_UPDATES_TABLE = ""
        + "DROP TABLE " + UPDATES_TABLE
        ;
        
    public static final String DROP_SEL_100_SEQ_TABLE = ""
        + "DROP TABLE " + SEL_100_SEQ_TABLE
        ;
        
    public static final String DROP_SEL_100_RND_TABLE = ""
        + "DROP TABLE " + SEL_100_RND_TABLE
        ;

    public BenchmarkDDL(String string) {
        super(string);
    }
}