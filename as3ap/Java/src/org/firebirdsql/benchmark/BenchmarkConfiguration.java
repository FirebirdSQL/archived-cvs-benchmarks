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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;

/**
 * Configuration for the benchmark suite.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class BenchmarkConfiguration {
    
    public static final String SUITE_CONFIGURATION_RESOURCE = 
        "org/firebirdsql/benchmark/benchmarkSuite.properties";

    public static final String DATA_PATH_PROPERTY = "dataPath";
    public static final String DATABASE_PATH_PROPERTY = "databasePath";
    public static final String USER_NAME_PROPERTY = "userName";
    public static final String PASSWORD_PROPERTY = "password";
    public static final String TRANSACTION_ISOLATION = "txIsolation";
    
    public static final String TPB_MAPPING_PROPERTY = "tpbMapping";
    
    public static final String POOLING_PROPERTY = "pooling";
    public static final String NO_POOLING_STR = "none";
    public static final String CONNECTION_POOLING_STR = "connection";
    public static final String STATEMENT_POOLING_STR = "statement";
    
    public static final String MAX_CONNECTIONS = "maxConnections";
    public static final String USER_COUNT = "userCount";
    
    public static final int UNKNOWN_POOLING = 0;
    public static final int NO_POOLING = 1;
    public static final int CONNECTION_POOLING = 2;
    public static final int STATEMENT_POOLING = 3;
    
    public static final String BACKGROUND_TEST_DURATION = "bgDuration";
    public static final String MEASURMENT_TEST_DURARION = "perfDuration";
    public static final String SLEEP_DURATION = "sleepDuration";
    
    public static final String RECREATE_TABLE_AS_CLEANUP = "recreateTable";
    
    public static final String JDBC_DRIVER_CLASS_NAME = "jdbcDriver";
    public static final String JDBC_DRIVER_URL = "jdbcUrl";
    
    public static final String FIXTURE_CLASS_NAME = "fixtureClassName";
    
    public static final String CUSTOM_PROPERTY = "custom";
    
    private static final Properties RES = new Properties();
    static {
        
        ClassLoader cl = BenchmarkConfiguration.class.getClassLoader();
        InputStream in = cl.getResourceAsStream(SUITE_CONFIGURATION_RESOURCE);
        
        if (in == null) {
            cl = Thread.currentThread().getContextClassLoader();
            in = cl.getResourceAsStream(SUITE_CONFIGURATION_RESOURCE);
        }
        
        try {
            if (in != null)
                RES.load(in);
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }
        
    private static final BenchmarkConfiguration config = 
        new BenchmarkConfiguration();
        
    private BenchmarkConfiguration() {
        // empty
    }
    
    private String getProperty(String propertyName) {
        return getProperty(propertyName, null);
    }
    
    private String getProperty(String propertyName, String defaultValue) {
        String systemProperty = System.getProperty(propertyName);
        
        if (systemProperty != null)
            return systemProperty;
        
        return RES.getProperty(propertyName, defaultValue);
    }
    
    public static BenchmarkConfiguration getConfiguration() {
        return config;
    }
    
    public String getDataPath() {
        return getProperty(DATA_PATH_PROPERTY, ".");
    }
    
    public String getUserName() {
        return getProperty(USER_NAME_PROPERTY, "SYSDBA");
    }
    
    public String getPassword() {
        return getProperty(PASSWORD_PROPERTY, "masterkey");
    }
    
    public int getTransactionIsolation() {
        String isolation = getProperty(TRANSACTION_ISOLATION, null);
        if (isolation == null)
            return -1;
        
        if ("TRANSACTION_NONE".equals(isolation))
            return Connection.TRANSACTION_NONE;
        else
        if ("TRANSACTION_READ_UNCOMMITTED".equals(isolation))
            return Connection.TRANSACTION_READ_UNCOMMITTED;
        else
        if ("TRANSACTION_READ_COMMITTED".equals(isolation))
            return Connection.TRANSACTION_READ_COMMITTED;
        else
        if ("TRANSACTION_REPEATABLE_READ".equals(isolation))
            return Connection.TRANSACTION_REPEATABLE_READ;
        else
        if ("TRANSACTION_SERIALIZABLE".equals(isolation))
            return Connection.TRANSACTION_SERIALIZABLE;
        else
            return -1;
    }
    
    public int getPoolingType() {
        String poolingType = getProperty(POOLING_PROPERTY, null);
        
        if(NO_POOLING_STR.equals(poolingType))
            return NO_POOLING;
        else
        if (CONNECTION_POOLING_STR.equals(poolingType))
            return CONNECTION_POOLING;
        else
        if (STATEMENT_POOLING_STR.equals(poolingType))
            return STATEMENT_POOLING;
        else
            return UNKNOWN_POOLING;
    }
    
    private int getIntProperty(String key, int defaultValue) {
        String strValue = getProperty(key);
        
        if (strValue == null)
            return defaultValue;
            
        try {
            int value = Integer.parseInt(strValue);
            
            return value;
        } catch(NumberFormatException ex) {
            return defaultValue;
        }

    }
    
    private boolean getBooleanProperty(String key, boolean defaultValue) {
        String strValue = getProperty(key);
        
        if ("true".equals(strValue))
            return true;
        else
        if ("false".equals(strValue))
            return false;
        else
            return defaultValue;
    }
    
    public int getBackgroundTestDuration() {
        return getIntProperty(BACKGROUND_TEST_DURATION, 15 * 60)  * 1000;
    }
    
    public int getPerformanceDuration() {
        return getIntProperty(MEASURMENT_TEST_DURARION, 5 * 60)  * 1000;
    }
    
    public int getMaxConnections() {
        return getIntProperty(MAX_CONNECTIONS, 20);
    }
    
    public boolean isRecreateTableAsCleanup() {
        return getBooleanProperty(RECREATE_TABLE_AS_CLEANUP, false);
    }
    
    public int getUserCount() {
        return getIntProperty(USER_COUNT, 1);
    }
    
    public String getDriverClassName() {
        return getProperty(JDBC_DRIVER_CLASS_NAME);
    }
    
    public String getJdbcUrl() {
        return getProperty(JDBC_DRIVER_URL);
    }
    
    public int getSleepDuration() {
        return getIntProperty(SLEEP_DURATION, 0);
    }
    
    public String getFixtureClassName() {
        return getProperty(FIXTURE_CLASS_NAME, JdbcFixture.class.getName());
    }
    
    public String getCustomProperty(String property) {
        return getCustomProperty(property, null);
    }
    
    public String getCustomProperty(String property, String defaultValue) {
        return getProperty(CUSTOM_PROPERTY + "." + property, defaultValue);
    }
    
    public int getCustomIntProperty(String property, int defaultValue) {
        return getIntProperty(CUSTOM_PROPERTY + "." + property, defaultValue);
    }
    
    public boolean getCustomBooleanProperty(String property, boolean defaultValue) {
        return getBooleanProperty(CUSTOM_PROPERTY + "." + property, defaultValue);
    }
}
