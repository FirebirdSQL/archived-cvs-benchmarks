//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
//
// Author: Carlos Guzman Alvarez <carlosga@telefonica.net>
//
// Distributable under LGPL license.
// You may obtain a copy of the License at http://www.gnu.org/copyleft/lgpl.html
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// LGPL License for more details.
//
// This file was created by members of the Firebird development team.
// All individual contributions remain the Copyright (C) of those
// individuals.  Contributors to this file are either listed here or
// can be obtained from a CVS history command.
//
// (c) 2003. All rights reserved.
//
// For more information please see http://www.firebirdsql.org
//

using System;
using System.Data;

namespace AS3AP.BenchMark
{
	public class Sql92TestSuite : BaseTestSuite
	{
		#region Constructors

		public Sql92TestSuite(BenchMarkConfiguration configuration) : base(configuration)
		{
			this.testSuiteName = "SQL87";
		}		

		#endregion

		#region Methods

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
					"select uniques.col_signed, uniques.col_name, "			+
						"hundred.col_signed, hundred.col_name "				+
					"from uniques "											+
						"join hundred ON uniques.col_key = hundred.col_key "+
					"where uniques.col_key = 1000");
		
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
					"select uniques.col_signed, uniques.col_name, "				+
						"hundred.col_signed, hundred.col_name "					+
					"from uniques "												+
						"join hundred on uniques.col_code = hundred.col_code "	+
					"where uniques.col_code = 'BENCHMARKS'");

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
					"select uniques.col_signed, uniques.col_name, "				+
					"hundred.col_signed, hundred.col_name "						+
					"from uniques "												+
					"join hundred ON uniques.col_address = hundred.col_address "+
					"where uniques.col_address = 'SILICON VALLEY'");

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
					"select uniques.col_signed, uniques.col_date, "			+
						"hundred.col_signed, hundred.col_date, "			+
						"tenpct.col_signed, tenpct.col_date "				+
					"from uniques "											+
						"join hundred on uniques.col_key = hundred.col_key "+
						"join tenpct on uniques.col_key = tenpct.col_key "	+
					"where uniques.col_key = 1000");				

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
					"select uniques.col_signed, uniques.col_date, "				+
						"hundred.col_signed, hundred.col_date, "				+
						"tenpct.col_signed, tenpct.col_date "					+
					"from uniques "												+
						"join hundred on uniques.col_code = hundred.col_code "	+
						"join tenpct on uniques.col_code = tenpct.col_code "	+
					"where uniques.col_code = 'BENCHMARKS'");				

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
					"select uniques.col_date, hundred.col_date, "			+
						"tenpct.col_date, updates.col_date "				+
					"from uniques "											+
						"join hundred on uniques.col_key = hundred.col_key "+
						"join tenpct on uniques.col_key = tenpct.col_key "	+
						"join updates on uniques.col_key = updates.col_key "+
					"where uniques.col_key = 1000");

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
					"select uniques.col_date, hundred.col_date, "				+
						"tenpct.col_date, updates.col_date "					+
					"from uniques "												+
						"join hundred on uniques.col_code = hundred.col_code "	+
						"join tenpct on uniques.col_code = tenpct.col_code "	+
						"join updates on uniques.col_code = updates.col_code "	+
						"where uniques.col_code = 'BENCHMARKS'");

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
