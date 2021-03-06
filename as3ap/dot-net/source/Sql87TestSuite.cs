//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
// Copyright (C) 2003-2006  Carlos Guzman Alvarez
//
// Distributable under LGPL license.
// You may obtain a copy of the License at http://www.gnu.org/copyleft/lesser.html
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//

using System;
using System.Data;

namespace DatabaseBenchmark
{
	public class Sql87TestSuite : BaseTestSuite
	{
		#region � Constructors �

		public Sql87TestSuite(BenchMarkConfiguration configuration) : base(configuration)
		{
			this.testSuiteName = "SQL87";
		}

		#endregion

		#region � Methods �

		/* AS3AP - An ANSI SQL Standard Scalable and Portable Benchmark for Relational Database Systems
		 * 
		 * Joins (Table 7.3)
		 */

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public override void join_2_cl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select uniques.col_signed, uniques.col_name, "	+
					"hundred.col_signed, hundred.col_name "			+
					"from uniques, hundred "						+
					"where uniques.col_key = hundred.col_key "		+
					"and uniques.col_key = 1000");

				if (count != 1)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				base.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public override void join_2_ncl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select uniques.col_signed, uniques.col_name, "	+
					"hundred.col_signed, hundred.col_name "			+
					"from uniques, hundred "						+
					"where uniques.col_code = hundred.col_code "	+
					"and uniques.col_code = 'BENCHMARKS'"			);				

				if (count != 1)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				base.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public override void join_2() 
		{
			try
			{	
				int count = this.ExecuteReader(
					"select uniques.col_signed, uniques.col_name, "		+
					"hundred.col_signed, hundred.col_name "				+
					"from uniques, hundred "							+
					"where uniques.col_address = hundred.col_address "	+
					"and uniques.col_address = 'SILICON VALLEY'");

				if (count != 1)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				base.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public override void join_3_cl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select uniques.col_signed, uniques.col_date, "	+
					"hundred.col_signed, hundred.col_date, "		+
					"tenpct.col_signed, tenpct.col_date "			+
					"from uniques, hundred, tenpct "				+
					"where uniques.col_key = hundred.col_key "		+
					"and uniques.col_key = tenpct.col_key "			+
					"and uniques.col_key = 1000");

				if (count != 1)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				base.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public override void join_3_ncl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select uniques.col_signed, uniques.col_date, "		+
					"hundred.col_signed, hundred.col_date, "			+
					"tenpct.col_signed, tenpct.col_date "				+
					"from uniques, hundred, tenpct "					+
					"where uniques.col_code = hundred.col_code "		+
					"and uniques.col_code = tenpct.col_code "			+
					"and uniques.col_code = 'BENCHMARKS'");
				
				if (count != 1)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				base.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public override void join_4_cl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select uniques.col_date, hundred.col_date, "	+
					"tenpct.col_date, updates.col_date "			+
					"from uniques, hundred, tenpct, updates "		+
					"where uniques.col_key = hundred.col_key "		+
					"and uniques.col_key = tenpct.col_key "			+
					"and uniques.col_key = updates.col_key "		+
					"and uniques.col_key = 1000");

				if (count != 1)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				base.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public override void join_4_ncl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select uniques.col_date, hundred.col_date, "	+
					"tenpct.col_date, updates.col_date "			+
					"from uniques, hundred, tenpct, updates "		+
					"where uniques.col_code = hundred.col_code "	+
					"and uniques.col_code = tenpct.col_code "		+
					"and uniques.col_code = updates.col_code "		+
					"and uniques.col_code = 'BENCHMARKS'");

				if (count != 1)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				base.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public override void join_1_10() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select uniques.col_key, uniques.col_name, " +
					"tenpct.col_name, tenpct.col_signed " +
					"from uniques, tenpct " +
					"where uniques.col_key = tenpct.col_signed");

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				base.testFailed = true;
			}
		}

		#endregion
	}
}
