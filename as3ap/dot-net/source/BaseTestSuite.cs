//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
//
// Author: Carlos Guzmán Álvarez <carlosga@telefonica.net>
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
using System.Threading;
using System.Text;
using System.Reflection;

using AS3AP.BenchMark.Backends;

namespace AS3AP.BenchMark
{
	public abstract class BaseTestSuite : ITestSuite
	{
		#region FIELDS

		private const string baseTableStructure =
			"col_key     int             not null, "	+
			"col_int     int             not null, "	+
			"col_signed  int             not null, "	+
			"col_float   float           not null, "	+
			"col_double  float			 not null, "	+
			"col_decim   numeric(18,2)   not null, "	+
			"col_date    char(20)        not null, "	+
			"col_code    char(10)        not null, "	+
			"col_name    char(20)        not null, "	+
			"col_address varchar(80)     not null";


		private IBackend	backend;
		private bool		doIndexes;
		private bool		testFailed = false;
		private object		testResult = 0;
		private int			tupleCount = 0;

		#endregion

		#region PROPERTIES

		public object TestResult
		{
			get { return testResult; }
			set { testResult = value; }
		}

		public bool TestFailed
		{
			get { return testFailed; }
			set { testFailed = value; }
		}

		public IBackend Backend
		{
			get { return backend; }
		}

		public int TupleCount
		{
			get { return tupleCount; }
			set { tupleCount = value;}
		}

		#endregion

		#region CONSTRUCTORS

		public BaseTestSuite(string backendName)
		{
			Assembly aseembly = Assembly.Load("AS3AP.BenchMark.Backends");

			this.doIndexes	= true;			

			this.backend	= (IBackend)Activator.CreateInstance(
											aseembly.GetType(backendName));
		}

		#endregion

		#region ABSTRACT_METHODS

		public abstract void agg_simple_report();

		public abstract void join_2();
		
		public abstract void join_2_cl();

		public abstract void join_2_ncl();

		public abstract void join_3_cl();

		public abstract void join_3_ncl();

		public abstract void join_4_cl();

		public abstract void join_4_ncl();

		#endregion

		#region MISC_METHODS

		public void CloseBackendLogger()
		{
			backend.CloseLogger();
		}

		public void setup_database()
		{
			try
			{
				backend.TransactionBegin();

				// Remove reportview
				backend.ExecuteStatement("drop view reportview");

				// Remove saveupdates table
				backend.ExecuteStatement("drop table saveupdates");

				backend.TransactionCommit();

				// Create indexes for updates table				
				create_idx_updates_double_bt();
				create_idx_updates_decim_bt();
			}
			catch (Exception)
			{
			}
		}

		public void CreateData()
		{
			try
			{
				backend.CreateData();
			}
			catch(Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		public void LoadData()
		{
			try
			{
				backend.LoadData();
			}
			catch(Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		public int CountRows(string table)
		{
			StringBuilder	commandText = new StringBuilder();
			int				count = 0;

			commandText.AppendFormat("select count(col_key) from {0}", table);
			
			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(commandText.ToString());
				if (backend.CursorFetch())
				{
					count = backend.Cursor.GetInt32(0);					
				}
			}
			catch(Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return count;
		}

		public void SetIsolationLevel(string methodName)
		{
			IsolationLevel			isolationLevel = IsolationLevel.ReadCommitted;
			IsolationLevelAttribute att;

			try
			{
				att = (IsolationLevelAttribute)Attribute.GetCustomAttribute(GetType().GetMethod(methodName),
					typeof(IsolationLevelAttribute));
				isolationLevel = att.IsolationLevel;
			}
			catch
			{				
			}
			finally
			{
				backend.Isolation = isolationLevel;
			}
		}

		#endregion

		#region SINGLE_USER_TESTS_METHODS

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_create_view() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("create view "									+
					"reportview(col_key,col_signed,col_date,col_decim, "	+
					"col_name,col_code,col_int) as "						+
					"select updates.col_key, updates.col_signed, "			+
					"updates.col_date, updates.col_decim, "					+
					"hundred.col_name, hundred.col_code, "					+
					"hundred.col_int "										+
					"from updates, hundred "								+
					"where updates.col_key = hundred.col_key");
				backend.TransactionCommit();
			}
			catch(Exception)
			{				
				testFailed = true;
			}

			testResult = 0; 
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_func() 
		{		
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select min(col_key) from hundred group by col_name");			
				while (backend.CursorFetch()) 
				{
					count++;
				}
			}
			catch(Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();				
			}

			testResult = count; 
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_info_retrieval() 
		{
			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select count(col_key) "									+
					"from tenpct "												+
					"where col_name = 'THE+ASAP+BENCHMARKS+' "					+
					"and col_int <= 100000000 "									+
					"and col_signed between 1 and 99999999 "					+
					"and not (col_float between -450000000 and 450000000) "		+
					"and col_double > 600000000 "								+
					"and col_decim < -600000000");

				backend.CursorFetch();

				testResult = backend.Cursor.GetValue(0);
			}
			catch(Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_scal() 
		{	
			try
			{
				backend.TransactionBegin();
				backend.CursorOpen("select min(col_key) from uniques");
				
				backend.CursorFetch();
				
				testResult = backend.Cursor.GetValue(0);
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
				testResult = -1;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_subtotal_report() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select avg(col_signed), min(col_signed), max(col_signed), "	+
					"max(col_date), min(col_date), "								+
					"count(distinct col_name), count(col_name), "					+
					"col_code, col_int "											+
					"from reportview "												+
					"where col_decim >980000000 "									+
					"group by col_code, col_int");
				
				while (backend.CursorFetch())
				{  
					count++;
				}
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_total_report() 
		{			
			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select avg(col_signed), min(col_signed), max(col_signed), "	+
					"max(col_date), min(col_date), "						+
					"count(distinct col_name), count(col_name), "			+
					"count(col_code), count(col_int) "						+
					"from reportview "												+
					"where col_decim >980000000");

				if (backend.CursorFetch())
				{
					testResult = backend.Cursor.GetValue(0);
				}
				else
				{
					backend.CursorClose();					
					backend.TransactionRollback();

					testFailed = true;
					testResult = -1;
				}
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
				testResult = -1;
			}
			finally
			{
				backend.CursorClose();
				if (!testFailed)
				{
					backend.TransactionCommit();
				}
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_append() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("insert into updates select * from saveupdates");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_delete() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("delete from updates where col_key < 0");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_modify() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("update updates "	+
					"set col_key = col_key - 100000 "		+
					"where col_key between 5000 and 5999");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_save()
		{
			try
			{
				backend.CreateTable("saveupdates", baseTableStructure, null);

				backend.TransactionBegin();
				backend.ExecuteStatement("insert into saveupdates select * "	+
							"from updates where col_key between 5000 and 5999");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_hundred_code_h() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("hundred_code_h"	, 
						"hundred"		, 
						"col_code");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_hundred_foreign() 
		{
			if (doIndexes) 
			{  
				try
				{
					backend.CreateForeignKey("hundred", 
											"fk_hundred_updates"	, 
											"col_signed"			, 
											"updates"				, 
											"col_key");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_hundred_key_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexCluster("hundred_key_bt"	, 
						"hundred"		, 
						"col_key");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_code_h() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexHash("tenpct_code_h"	, 
											"tenpct"		, 
											"col_code");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_decim_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("tenpct_decim_bt"	, 
												"tenpct"		, 
												"col_decim");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_double_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("tenpct_double_bt"	, 
												"tenpct"		, 
												"col_double");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0; 
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_float_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("tenpct_float_bt"	, 
												"tenpct"		, 
												"col_float");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_int_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("tenpct_int_bt"	, 
												"tenpct"		, 
												"col_int");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_key_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexCluster("tenpct_key_bt"	, 
												"tenpct"		, 
												"col_key");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_key_code_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("tenpct_key_code_bt"	,
												"tenpct"			,
												"col_key, col_code");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_name_h() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexHash("tenpct_name_h"	, 
											"tenpct"	,
											"col_name");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			} 
			
			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_signed_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("tenpct_signed_bt"	, 
												"tenpct"		,
												"col_signed");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			} 

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tiny_key_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("tiny_key_bt"	, 
												"tiny"		, 
												"col_key");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_uniques_code_h() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexHash("uniques_code_h"	, 
											"uniques"			, 
											"col_code");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		} 

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_uniques_key_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexCluster("uniques_key_bt"	, 
												"uniques"		, 
												"col_key");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		} 

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_updates_code_h() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexHash("updates_code_h"	, 
											"updates"			, 
											"col_code");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_updates_decim_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("updates_decim_bt"	, 
												"updates"		,
												"col_decim");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_updates_double_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("updates_double_bt"	, 
												"updates"			,
												"col_double");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			} 
			testResult = 0;
		} 

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_updates_int_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexBtree("updates_int_bt"	, 
												"updates"		, 
												"col_int");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		} 

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_updates_key_bt() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.CreateIndexCluster("updates_key_bt"	, 
												"updates"		, 
												"col_key");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_tables() 
		{
			try
			{
				backend.CreateTable("uniques", baseTableStructure, "col_key");
				backend.CreateTable("hundred", baseTableStructure, "col_key");
				backend.CreateTable("updates", baseTableStructure, "col_key");
				backend.CreateTable("tenpct" , baseTableStructure, "col_key,col_code");

				backend.CreateTable( 
					"tiny"					,
					"col_key int not null",
					"col_key");
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void drop_updates_keys() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.TransactionBegin();
					backend.ExecuteStatement("drop index updates_int_bt");
					backend.ExecuteStatement("drop index updates_double_bt");
					backend.ExecuteStatement("drop index updates_decim_bt");
					backend.ExecuteStatement("drop index updates_code_h");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void integrity_test() 
		{
			try
			{
				backend.CreateTable("integrity_temp", baseTableStructure, null);

				backend.TransactionBegin();
				backend.ExecuteStatement("insert into integrity_temp select * from hundred where col_int = 0");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("update hundred set col_signed = '-500000000' where col_int = 0");
				backend.TransactionCommit();

				testFailed = true;				
				testResult = 0;	
			}
			catch (Exception)
			{				
			}

			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("delete from hundred where col_int = 0");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("insert into hundred select * from integrity_temp");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("drop table integrity_temp");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void proj_100() 
		{
			int count = 0;

			lock (backend)
			{
				try
				{
					backend.TransactionBegin();
					backend.CursorOpen(
						"select distinct col_address, col_signed from hundred");
				
					while (backend.CursorFetch()) 
					{
						count++;
					}				
				}
				catch (Exception)
				{
					backend.TransactionRollback();
					testFailed = true;
				}
				finally
				{
					backend.CursorClose();
					backend.TransactionCommit();
				}
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void proj_10pct() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen("select distinct col_signed from tenpct");
				
				while (backend.CursorFetch()) 
				{
					count++;
				}				
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_1_cl() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_key = 1000");
				
				while (backend.CursorFetch()) 
				{
					count++;
				}			
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			testResult = count; 
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_1_ncl() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen( 
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_code = 'BENCHMARKS'");
				
				while (backend.CursorFetch()) 
				{  
					count++;
				}
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_100_cl() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_key <= 100");
				
				while (backend.CursorFetch()) 
				{
					count++;
				}
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_100_ncl() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_int <= 100");
				
				while (backend.CursorFetch()) 
				{
					count++;
				}
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}


			testResult = count;
		}
	
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	sel_10pct_ncl() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen( 
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "						+
					"from tenpct "									+
					"where col_name = 'THE+ASAP+BENCHMARKS+'");
				
				while (backend.CursorFetch()) 
				{  
					count++;
				}
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	sel_variable_select(long foo) 
		{
			StringBuilder	lineBuf = new StringBuilder();
			int				count = 0;


			try
			{
				lineBuf.AppendFormat(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "							+
					"from tenpct "									+
					"where col_signed < {0}",						+
					foo);

				backend.TransactionBegin();
				backend.CursorOpen(lineBuf.ToString());
				
				while (backend.CursorFetch()) 
				{
					count++;
				}
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			testResult = count;
		}


		public void sel_variable_select_high() 
		{
			sel_variable_select(-250000000);
		}


		public void sel_variable_select_low() 
		{
			sel_variable_select(-500000000);
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void table_scan() 
		{
			int count = 0;

			try
			{			
				backend.TransactionBegin();
				backend.CursorOpen("select * from uniques where col_int = 1");
				
				while (backend.CursorFetch()) 
				{  
					count++;
				}
			}
			catch (Exception)
			{
				backend.TransactionRollback();
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_app_t_end() 
		{
			int count = 0;
		
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("insert into updates "		+
					"values (1000000001, 50005, 50005, 50005.00, "	+
					"50005.00, 50005.00, '1/1/1988', "				+
					"'CONTROLLER', 'ALICE IN WONDERLAND', "			+
					"'UNIVERSITY OF ILLINOIS AT CHICAGO')"); 				
				backend.TransactionCommit();
				
				count++;
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = count; 
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_app_t_mid() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("insert into updates "							+
					"values (5005, 5005, 50005, 50005.00, 50005.00, "	+
					"50005.00, '1/1/1988', 'CONTROLLER', "				+
					"'ALICE IN WONDERLAND', "							+
					"'UNIVERSITY OF ILLINOIS AT CHICAGO')");
				backend.TransactionCommit();

				count++;
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_append_duplicate() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.TransactionBegin();
					backend.ExecuteStatement( "insert into updates  "		+
						"values (6000, 0, 60000, 39997.90, "				+
						"50005.00, 50005.00, "								+
						"'11/10/1985', 'CONTROLLER', "						+
						"'ALICE IN WONDERLAND', "							+
						"'UNIVERSITY OF ILLINOIS AT CHICAGO')"); 
					backend.TransactionCommit();

					testFailed = true;
				}
				catch (Exception)
				{
				}
			}

			testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_del_t_end() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("delete from updates where col_key = -1000"); 
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0; 
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_del_t_mid() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("delete from updates where (col_key='5005') or (col_key='-5000')");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_cod() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("update updates "	+
					"set col_code = 'SQL+GROUPS' "			+
					"where col_key = 5005");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_end() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement( "update updates "	+
					"set col_key = -1000 where col_key = 1000000001");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_int() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("update updates set col_int = 50015 where col_key = 5005");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0; 
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_mid() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("update updates set col_key = '-5000' where col_key = 5005");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_remove_duplicate() 
		{
			try
			{
				backend.TransactionBegin();
				backend.ExecuteStatement("delete from updates where col_key = 6000 and col_int = 0");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		#endregion

		#region MULTIUSER_AND_CROSS_SECTION_TESTS_METHODS

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void o_mode_tiny()
		{	
			int count = 0;

			lock (backend)
			{
				try
				{
					backend.TransactionBegin();
					backend.CursorOpen("select * from tiny");
				
					while (backend.CursorFetch())
					{
						count++;
					}
				}
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{
					backend.CursorClose();
					backend.TransactionCommit();
				}
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void o_mode_100k()
		{
			int count = 0;

			lock (backend)
			{
				try
				{
					backend.TransactionBegin();
					backend.CursorOpen("select * from hundred where col_key<=1000");
				
					while (backend.CursorFetch())
					{
						count++;
					}
				}
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{
					backend.CursorClose();
					backend.TransactionCommit();
				}
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_checkmod_100_rand() 
		{
			object count = 0;
			
			lock (backend)
			{
				try
				{
					backend.TransactionBegin();
					backend.CursorOpen(
						"select count(*) from updates, sel100rand "		+
						"where updates.col_int=sel100rand.col_int "		+
						"and not updates.col_double=sel100rand.col_double");
				
					backend.CursorFetch();

					count = backend.Cursor.GetValue(0);
				}
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{					
					backend.CursorClose();
					backend.TransactionCommit();
				}
			}

			if (Convert.ToInt32(count) != 100) 
			{
				testFailed = true;
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_drop_sel100_rand() 
		{
			lock (backend)
			{
				try
				{
					backend.TransactionBegin();
					backend.ExecuteStatement("drop table sel100rand");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_checkmod_100_seq() 
		{
			object count = 0;

			lock (backend)
			{
				try
				{
					backend.TransactionBegin();
					backend.CursorOpen(
						"select count(*) from updates, sel100seq "		+
						"where updates.col_key=sel100seq.col_key "		+
						"and not updates.col_double=sel100seq.col_double");
				
					backend.CursorFetch();
				
					count = backend.Cursor.GetValue(0); 
				}
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{
					backend.CursorClose();
					backend.TransactionCommit();
				}
			}

			if (Convert.ToInt32(count) != 100) 
			{
				testFailed = true;
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_drop_sel100_seq() 
		{
			lock (backend)
			{
				try
				{
					backend.TransactionBegin();
					backend.ExecuteStatement("drop table sel100seq");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_ir_select()
		{
			StringBuilder	lineBuf = new StringBuilder();
			int				count = 0;
			int				r;
				
			Random randNumber = new Random(unchecked((int)DateTime.Now.Ticks));
			r = 1;

			lock (backend)
			{
				try
				{
					while (r == 1)
					{
						r = randNumber.Next(0, tupleCount);   // there IS no key 1
					}
			
					lineBuf.AppendFormat(
						"select col_key, col_code, col_date, col_signed, col_name "	+
						"from updates where col_key = {0}",	r);

					backend.TransactionBegin();
					backend.CursorOpen(lineBuf.ToString());
					if (!backend.CursorFetch())
					{
						testFailed = true;
						count = 0;
					}
					else
					{
						count++;
					}
				}	
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{
					backend.CursorClose();
					backend.TransactionCommit();
				}
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_mod_100_rand() 
		{
			int count = 0;

			lock (backend)
			{
				try
				{
					backend.TransactionBegin();
					backend.ExecuteStatement("update updates "	+
						"set col_double=col_double+100000000 "	+
						"where col_int between 1001 and 1100");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_mod_100_seq() 
		{
			lock (backend)
			{
				try
				{
					backend.TransactionBegin();					
					backend.ExecuteStatement("update updates "		+
						"set col_double = col_double+100000000 "	+
						"where col_key between 1001 and 1100");
					backend.TransactionRollback();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void mu_oltp_update()
		{
			StringBuilder   lineBuf = new StringBuilder();
			int				r;

			lock (backend)
			{
				try
				{
					Random randomNumber = new Random(unchecked((int)DateTime.Now.Ticks));
					r = 1;
					while (r == 1)
					{
						r = randomNumber.Next(0, tupleCount);    // There IS no col_key 1
					}	

					lineBuf.AppendFormat(
						"update updates set col_signed = col_signed + 1 " +
						"where col_key = {0}", r);

					backend.TransactionBegin();
					backend.ExecuteStatement(lineBuf.ToString());
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_sel_100_rand() 
		{
			lock (backend)
			{
				try
				{
					backend.CreateTable("sel100rand", baseTableStructure, null);

					backend.TransactionBegin();
					backend.ExecuteStatement("insert into sel100rand select * from updates "	+
						"where updates.col_int between 1001 and 1100");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_sel_100_seq() 
		{
			lock (backend)
			{
				try
				{
					backend.CreateTable("sel100seq", baseTableStructure, null);

					backend.TransactionBegin();
					backend.ExecuteStatement("insert into sel100seq select * from updates "	+
						"where updates.col_key between 1001 and 1100");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_unmod_100_rand() 
		{
			lock (backend)
			{
				try
				{
					backend.TransactionBegin();
					backend.ExecuteStatement("update updates "	+
						"set col_double=col_double-100000000 "	+
						"where col_int between 1001 and 1100");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_unmod_100_seq() 
		{
			lock (backend)
			{
				try
				{
					backend.TransactionBegin();
					backend.ExecuteStatement("update updates "	+
						"set col_double=col_double-100000000 "	+
						"where col_key between 1001 and 1100");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			testResult = 0; 
		}

		#endregion
	}
}
