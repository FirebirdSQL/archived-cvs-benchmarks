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
 * This suite creates benchmark database and loads data into it.
 * 
 * @author <a href="mailto:rrokytskyy@users.sourceforge.net">Roman Rokytskyy</a>
 */
public class LoadDataSuite extends BenchmarkSuite {


	/**
     * Fill this test suite.
	 */
	public void fillSuite() {
		addTest(getFixture().createLoadTest("testLoadData"));

	}

	/**
     * Check if database should be created.
     * 
     * @return <code>true</code> always. 
	 */
	protected boolean isCreateDatabase() {
		return true;
	}

    /**
     * Run this benchmark suite.
     * 
     * @param args arguments to this program.
     */
    public static void main(String[] args) {
        BenchmarkListener listener = new BenchmarkListener();
        TestRunner.run(new LoadDataSuite().suite(), listener);
        listener.printStatistics(System.out);
    }

}
