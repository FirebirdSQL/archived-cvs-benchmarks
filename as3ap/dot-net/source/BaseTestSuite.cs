//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
//
// Author: Carlos Guzmn lvarez <carlosga@telefonica.net>
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
using System.IO;
using System.Data;
using System.Collections;
using System.Threading;
using System.Text;
using System.Reflection;
using System.Text.RegularExpressions;

using CSharp.Logger;

using Common.Data.Helper;

namespace AS3AP.BenchMark
{
	public enum IndexType
	{
		Btree,
		Clustered,
		Hash
	}

	#region DELEGATES

	public delegate void ResultEventHandler(object sender, TestResultEventArgs e);
	public delegate void ProgressEventHandler(object sender, ProgressMessageEventArgs e);

	#endregion

	public abstract class BaseTestSuite : ITestSuite
	{
		#region EVENTS

		public event 	ResultEventHandler		Result;
		public event 	ProgressEventHandler	Progress;

		#endregion

		#region FIELDS

		private Thread[] userProcess;

		private string baseTableStructure =
			"col_key     @INTEGER			not null, "	+
			"col_int     @INTEGER			not null, "	+
			"col_signed  @INTEGER			not null, "	+
			"col_float   @FLOAT				not null, "	+
			"col_double  @DOUBLE			not null, "	+
			"col_decim   @DECIMAL(18,2)		not null, "	+
			"col_date    @CHAR(20)			not null, "	+
			"col_code    @CHAR(10)			not null, "	+
			"col_name    @CHAR(20)			not null, "	+
			"col_address @VARCHAR(80)		not null";

		private BenchMarkConfiguration configuration;

		private	Logger		log;
		private int			iters		= 0;
		private int			timeToRun	= 15;
		private bool		disposed	= false;
		private int			tupleCount	= 0;		

		protected bool		testFailed	= false;
		protected object	testResult	= 0;
		
		protected string 	testSuiteName = String.Empty;

		private		IsolationLevel	isolation  = IsolationLevel.ReadCommitted;
		private		IDbConnection	connection;
		private		IDbTransaction	transaction;
		protected	IDataReader		cursor;
		private		IDbCommand		cmdCursor;
		private		DataHelper		dataHelper;

		#endregion

		#region PROPERTIES

		public Logger Log
		{
			get { return log; }
			set { log = value; }
		}

		public BenchMarkConfiguration Configuration
		{
			get { return configuration; }
			set { configuration = value; }
		}

		public int TupleCount
		{
			get { return tupleCount; }
			set { tupleCount = value;}
		}

		public string TestSuiteName
		{
			get { return testSuiteName; }
		}

		#endregion

		#region CONSTRUCTORS

		public BaseTestSuite(BenchMarkConfiguration configuration)
		{
			this.configuration	= configuration;

			// Set specific table structure
			baseTableStructure = baseTableStructure.Replace("@INTEGER", configuration.IntegerTypeName);
			baseTableStructure = baseTableStructure.Replace("@FLOAT", configuration.FloatTypeName);
			baseTableStructure = baseTableStructure.Replace("@DOUBLE", configuration.DoubleTypeName);
			baseTableStructure = baseTableStructure.Replace("@DECIMAL", configuration.DecimalTypeName);
			baseTableStructure = baseTableStructure.Replace("@CHAR", configuration.CharTypeName);
			baseTableStructure = baseTableStructure.Replace("@VARCHAR", configuration.VarcharTypeName);

			// Create the helper object
			dataHelper = DataHelperFactory.GetHelper(configuration.HelperType);
		}

		#endregion

		#region IDISPOSABLE_METHODS

		~BaseTestSuite()
		{
			Dispose(false);
		}

		private void Dispose(bool disposing)
		{
			if (!disposed)			
			{
				if (disposing)
				{
					try
					{
						// release any managed resources					
						if (cursor != null)
						{
							cursor.Close();
							cursor = null;
						}
			
						if (cmdCursor != null)
						{
							cmdCursor.Dispose();
							cmdCursor = null;
						}
			
						if (transaction != null)
						{
							rollbackTransaction();
							transaction = null;
						}
						
						if (connection != null)
						{
							connection.Close();
							connection = null;
						}
						if (log != null)
						{
							if (userProcess != null)
							{
								foreach(Thread process in userProcess)
								{
									process.Abort();
								}
							}							
						}

						configuration = null;
					}
					finally
					{
					}

					// release any unmanaged resources
				}
			}			
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		#endregion

		#region ABSTRACT_METHODS

		// public abstract void agg_simple_report();

		public abstract void join_2();
		
		public abstract void join_2_cl();

		public abstract void join_2_ncl();

		public abstract void join_3_cl();

		public abstract void join_3_ncl();

		public abstract void join_4_cl();

		public abstract void join_4_ncl();

		#endregion

		#region MISC_METHODS

		public void setup_database()
		{
			try
			{
				DatabaseConnect();

				beginTransaction();

				// Remove reportview
				executeStatement("drop view reportview");

				// Remove saveupdates table
				executeStatement("drop table saveupdates");

				commitTransaction();

				// Create indexes for updates table				
				create_idx_updates_double_bt();
				create_idx_updates_decim_bt();
			}
			catch (Exception)
			{
			}
			finally
			{
				DatabaseDisconnect();
			}
		}

		public int count_rows(string table)
		{
			StringBuilder	commandText = new StringBuilder();
			int				count = 0;

			commandText.AppendFormat("select count(col_key) from {0}", table);
			
			try
			{
				beginTransaction();
				cursorOpen(commandText.ToString());
				if (cursorFetch())
				{
					count = cursor.GetInt32(0);					
				}
			}
			catch(Exception)
			{
				testFailed = true;
			}
			finally
			{
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			return count;
		}

		public void set_isolation_level(string methodName)
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
				isolation = isolationLevel;
			}
		}

		#endregion

		#region SINGLE_USER_TESTS_METHODS

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_create_view() 
		{
			try
			{
				beginTransaction();
				executeStatement("create view "								+
					"reportview(col_key,col_signed,col_date,col_decim, "	+
					"col_name,col_code,col_int) as "						+
					"select updates.col_key, updates.col_signed, "			+
					"updates.col_date, updates.col_decim, "					+
					"hundred.col_name, hundred.col_code, "					+
					"hundred.col_int "										+
					"from updates, hundred "								+
					"where updates.col_key = hundred.col_key");
				commitTransaction();
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
				beginTransaction();
				cursorOpen(
					"select min(col_key) from hundred group by col_name");			
				while (cursorFetch()) 
				{
					count++;
				}
			}
			catch(Exception)
			{
				testFailed = true;
			}
			finally
			{
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			testResult = count; 
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_simple_report()
		{
			try
			{
				beginTransaction();
				cursorOpen(
					"select * "						+
					"from updates "					+
					"where updates.col_key in "		+
					"(select updates.col_key "		+
					"from updates, hundred "		+
					"where hundred.col_key = updates.col_key)");
				
				cursorFetch();

				testResult = cursor.GetValue(0);
			}
			catch (Exception)
			{
				testFailed = true;
			}
			finally
			{
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_info_retrieval() 
		{
			try
			{
				beginTransaction();
				cursorOpen(
					"select count(col_key) "									+
					"from tenpct "												+
					"where col_name = 'THE+ASAP+BENCHMARKS+' "					+
					"and col_int <= 100000000 "									+
					"and col_signed between 1 and 99999999 "					+
					"and not (col_float between -450000000 and 450000000) "		+
					"and col_double > 600000000 "								+
					"and col_decim < -600000000");

				cursorFetch();

				testResult = cursor.GetValue(0);
			}
			catch(Exception)
			{
				testFailed = true;
			}
			finally
			{
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_scal() 
		{	
			try
			{
				beginTransaction();
				cursorOpen("select min(col_key) from uniques");
				
				cursorFetch();
				
				testResult = cursor.GetValue(0);
			}
			catch (Exception)
			{
				testFailed = true;
				testResult = -1;
			}
			finally
			{
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}
		}



		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_subtotal_report() 
		{
			int count = 0;

			try
			{
				beginTransaction();
				cursorOpen(
					"select avg(col_signed), min(col_signed), max(col_signed), "	+
					"max(col_date), min(col_date), "								+
					"count(distinct col_name), count(col_name), "					+
					"col_code, col_int "											+
					"from reportview "												+
					"where col_decim >980000000 "									+
					"group by col_code, col_int");
				
				while (cursorFetch())
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_total_report() 
		{			
			try
			{
				beginTransaction();
				cursorOpen(
					"select avg(col_signed), min(col_signed), max(col_signed), "	+
					"max(col_date), min(col_date), "						+
					"count(distinct col_name), count(col_name), "			+
					"count(col_code), count(col_int) "						+
					"from reportview "												+
					"where col_decim >980000000");

				if (cursorFetch())
				{
					testResult = cursor.GetValue(0);
				}
				else
				{
					cursorClose();					
					rollbackTransaction();

					testFailed = true;
					testResult = -1;
				}
			}
			catch (Exception)
			{
				rollbackTransaction();
				testFailed = true;
				testResult = -1;
			}
			finally
			{
				cursorClose();
				if (!testFailed)
				{
					commitTransaction();
				}
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_append() 
		{
			try
			{
				beginTransaction();
				executeStatement("insert into updates select * from saveupdates");
				commitTransaction();
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
				beginTransaction();
				executeStatement("delete from updates where col_key < 0");
				commitTransaction();
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
				beginTransaction();
				executeStatement("update updates "	+
					"set col_key = col_key - 100000 "		+
					"where col_key between 5000 and 5999");
				commitTransaction();
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
				createTable("saveupdates", baseTableStructure, null);

				beginTransaction();
				executeStatement("insert into saveupdates select * "	+
							"from updates where col_key between 5000 and 5999");
				commitTransaction();
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree		,
										"hundred_code_h"	, 
										"hundred"			, 
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
			if (configuration.UseIndexes) 
			{  
				try
				{
					createForeignKey("hundred", 
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
			if (configuration.UseIndexes && configuration.SupportsClusteredIndexes) 
			{
				try
				{
					createIndex(IndexType.Clustered,"hundred_key_bt"	, 
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
			if (configuration.UseIndexes && configuration.SupportsHashIndexes) 
			{
				try
				{
					createIndex(IndexType.Hash,"tenpct_code_h"	, 
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree,"tenpct_decim_bt"	, 
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree,"tenpct_double_bt"	, 
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree,"tenpct_float_bt"	, 
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree,"tenpct_int_bt"	, 
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
			if (configuration.UseIndexes && configuration.SupportsClusteredIndexes) 
			{
				try
				{
					createIndex(IndexType.Clustered,"tenpct_key_bt"	, 
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree,"tenpct_key_code_bt"	,
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
			if (configuration.UseIndexes && configuration.SupportsHashIndexes) 
			{
				try
				{
					createIndex(IndexType.Hash,"tenpct_name_h"	, 
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree,"tenpct_signed_bt"	, 
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree,"tiny_key_bt"	, 
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
			if (configuration.UseIndexes && configuration.SupportsHashIndexes) 
			{
				try
				{
					createIndex(IndexType.Hash,"uniques_code_h"	, 
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
			if (configuration.UseIndexes && configuration.SupportsClusteredIndexes) 
			{
				try
				{
					createIndex(IndexType.Clustered,"uniques_key_bt"	, 
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
			if (configuration.UseIndexes && configuration.SupportsHashIndexes) 
			{
				try
				{
					createIndex(IndexType.Hash,"updates_code_h"	, 
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree,"updates_decim_bt"	, 
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree,"updates_double_bt"	, 
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
			if (configuration.UseIndexes) 
			{
				try
				{
					createIndex(IndexType.Btree,"updates_int_bt"	, 
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
			if (configuration.UseIndexes && configuration.SupportsClusteredIndexes) 
			{
				try
				{
					createIndex(IndexType.Clustered,"updates_key_bt"	, 
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
				createTable("uniques", baseTableStructure, "col_key");
				createTable("hundred", baseTableStructure, "col_key");
				createTable("updates", baseTableStructure, "col_key");
				createTable("tenpct" , baseTableStructure, "col_key,col_code");

				createTable( 
					"tiny"					,
					"col_key " + configuration.IntegerTypeName + " not null",
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
			if (configuration.UseIndexes) 
			{
				try
				{
					beginTransaction();
					executeStatement("drop index updates_int_bt");
					executeStatement("drop index updates_double_bt");
					executeStatement("drop index updates_decim_bt");
					if (configuration.SupportsHashIndexes)
					{
						executeStatement("drop index updates_code_h");
					}
					commitTransaction();
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
				createTable("integrity_temp", baseTableStructure, null);

				beginTransaction();
				executeStatement("insert into integrity_temp select * from hundred where col_int = 0");
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			try
			{
				beginTransaction();
				executeStatement("update hundred set col_signed = '-500000000' where col_int = 0");
				commitTransaction();

				testFailed = true;				
				testResult = 0;	
			}
			catch (Exception)
			{
			}

			try
			{
				beginTransaction();
				executeStatement("delete from hundred where col_int = 0");
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			try
			{
				beginTransaction();
				executeStatement("insert into hundred select * from integrity_temp");
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			try
			{
				beginTransaction();
				executeStatement("drop table integrity_temp");
				commitTransaction();
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

			try
			{
				beginTransaction();
				cursorOpen(
					"select distinct col_address, col_signed from hundred");
			
				while (cursorFetch()) 
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
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
				beginTransaction();
				cursorOpen("select distinct col_signed from tenpct");
				
				while (cursorFetch()) 
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_1_cl() 
		{
			int count = 0;

			try
			{
				beginTransaction();
				cursorOpen(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_key = 1000");
				
				while (cursorFetch()) 
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			testResult = count; 
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_1_ncl() 
		{
			int count = 0;

			try
			{
				beginTransaction();
				cursorOpen( 
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_code = 'BENCHMARKS'");
				
				while (cursorFetch()) 
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_100_cl() 
		{
			int count = 0;

			try
			{
				beginTransaction();
				cursorOpen(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_key <= 100");
				
				while (cursorFetch()) 
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_100_ncl() 
		{
			int count = 0;

			try
			{
				beginTransaction();
				cursorOpen(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_int <= 100");
				
				while (cursorFetch()) 
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}


			testResult = count;
		}
	
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	sel_10pct_ncl() 
		{
			int count = 0;

			try
			{
				beginTransaction();
				cursorOpen( 
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "						+
					"from tenpct "									+
					"where col_name = 'THE+ASAP+BENCHMARKS+'");
				
				while (cursorFetch()) 
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
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

				beginTransaction();
				cursorOpen(lineBuf.ToString());
				
				while (cursorFetch()) 
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
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
				beginTransaction();
				cursorOpen("select * from uniques where col_int = 1");
				
				while (cursorFetch()) 
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_app_t_end() 
		{
			int count = 0;
		
			try
			{
				beginTransaction();
				executeStatement("insert into updates "		+
					"values (1000000001, 50005, 50005, 50005.00, "	+
					"50005.00, 50005.00, '1/1/1988', "				+
					"'CONTROLLER', 'ALICE IN WONDERLAND', "			+
					"'UNIVERSITY OF ILLINOIS AT CHICAGO')"); 				
				commitTransaction();
				
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
				beginTransaction();
				executeStatement("insert into updates "							+
					"values (5005, 5005, 50005, 50005.00, 50005.00, "	+
					"50005.00, '1/1/1988', 'CONTROLLER', "				+
					"'ALICE IN WONDERLAND', "							+
					"'UNIVERSITY OF ILLINOIS AT CHICAGO')");
				commitTransaction();

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
			if (configuration.UseIndexes) 
			{
				try
				{
					beginTransaction();
					executeStatement( "insert into updates  "		+
						"values (6000, 0, 60000, 39997.90, "				+
						"50005.00, 50005.00, "								+
						"'11/10/1985', 'CONTROLLER', "						+
						"'ALICE IN WONDERLAND', "							+
						"'UNIVERSITY OF ILLINOIS AT CHICAGO')"); 
					commitTransaction();

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
				beginTransaction();
				executeStatement("delete from updates where col_key = -1000"); 
				commitTransaction();
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
				beginTransaction();
				executeStatement("delete from updates where (col_key='5005') or (col_key='-5000')");
				commitTransaction();
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
				beginTransaction();
				executeStatement("update updates "	+
					"set col_code = 'SQL+GROUPS' "			+
					"where col_key = 5005");
				commitTransaction();
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
				beginTransaction();
				executeStatement( "update updates "	+
					"set col_key = -1000 where col_key = 1000000001");
				commitTransaction();
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
				beginTransaction();
				executeStatement("update updates set col_int = 50015 where col_key = 5005");
				commitTransaction();
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
				beginTransaction();
				executeStatement("update updates set col_key = '-5000' where col_key = 5005");
				commitTransaction();
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
				beginTransaction();
				executeStatement("delete from updates where col_key = 6000 and col_int = 0");
				commitTransaction();
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

			try
			{
				beginTransaction();
				cursorOpen("select * from tiny");
			
				while (cursorFetch())
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void o_mode_100k()
		{
			int count = 0;

			try
			{
				beginTransaction();
				cursorOpen("select * from hundred where col_key<=1000");
			
				while (cursorFetch())
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_checkmod_100_rand() 
		{
			object count = 0;
			
			try
			{
				beginTransaction();
				cursorOpen(
					"select count(*) from updates, sel100rand "		+
					"where updates.col_int=sel100rand.col_int "		+
					"and not updates.col_double=sel100rand.col_double");
			
				cursorFetch();

				count = cursor.GetValue(0);
			}
			catch (Exception)
			{
				testFailed = true;
			}
			finally
			{					
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
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
			try
			{
				beginTransaction();
				executeStatement("drop table sel100rand");
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_checkmod_100_seq() 
		{
			object count = 0;

			try
			{
				beginTransaction();
				cursorOpen(
					"select count(*) from updates, sel100seq "		+
					"where updates.col_key=sel100seq.col_key "		+
					"and not updates.col_double=sel100seq.col_double");
			
				cursorFetch();
			
				count = cursor.GetValue(0); 
			}
			catch (Exception)
			{
				testFailed = true;
			}
			finally
			{
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
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
			try
			{
				beginTransaction();
				executeStatement("drop table sel100seq");
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
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

			try
			{
				while (r == 1)
				{
					r = randNumber.Next(0, tupleCount);   // there IS no key 1
				}
		
				lineBuf.AppendFormat(
					"select col_key, col_code, col_date, col_signed, col_name "	+
					"from updates where col_key = {0}",	r);

				beginTransaction();
				cursorOpen(lineBuf.ToString());
				if (!cursorFetch())
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
				cursorClose();
				if (testFailed)
				{
					rollbackTransaction();
				}
				else
				{
					commitTransaction();
				}
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_mod_100_rand() 
		{
			int count = 0;

			try
			{
				beginTransaction();
				executeStatement("update updates "	+
					"set col_double=col_double+100000000 "	+
					"where col_int between 1001 and 1100");
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_mod_100_seq() 
		{
			try
			{
				beginTransaction();					
				executeStatement("update updates "		+
					"set col_double = col_double+100000000 "	+
					"where col_key between 1001 and 1100");
				rollbackTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void mu_oltp_update()
		{
			StringBuilder   lineBuf = new StringBuilder();
			int				r;

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

				beginTransaction();
				executeStatement(lineBuf.ToString());
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_sel_100_rand() 
		{
			try
			{
				createTable("sel100rand", baseTableStructure, null);

				beginTransaction();
				executeStatement("insert into sel100rand select * from updates "	+
					"where updates.col_int between 1001 and 1100");
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_sel_100_seq() 
		{
			try
			{
				createTable("sel100seq", baseTableStructure, null);

				beginTransaction();
				executeStatement("insert into sel100seq select * from updates "	+
					"where updates.col_key between 1001 and 1100");
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_unmod_100_rand() 
		{
			try
			{
				beginTransaction();
				executeStatement("update updates "	+
					"set col_double=col_double-100000000 "	+
					"where col_int between 1001 and 1100");
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_unmod_100_seq() 
		{
			try
			{
				beginTransaction();
				executeStatement("update updates "	+
					"set col_double=col_double-100000000 "	+
					"where col_key between 1001 and 1100");
				commitTransaction();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			testResult = 0; 
		}

		#endregion

		#region AS3AP_METHODS

		public void create_database() 
		{
			databaseCreate();

			DatabaseConnect();

			runTest("create_tables");
			runTest("load_data");
			runTest("create_idx_uniques_key_bt");
			runTest("create_idx_updates_key_bt");
			runTest("create_idx_hundred_key_bt");
			runTest("create_idx_tenpct_key_bt");
			runTest("create_idx_tenpct_key_code_bt");
			runTest("create_idx_tiny_key_bt");
			runTest("create_idx_tenpct_int_bt");
			runTest("create_idx_tenpct_signed_bt");
			runTest("create_idx_uniques_code_h");
			runTest("create_idx_tenpct_double_bt");
			runTest("create_idx_updates_decim_bt");
			runTest("create_idx_tenpct_float_bt");
			runTest("create_idx_updates_int_bt");
			runTest("create_idx_tenpct_decim_bt");
			runTest("create_idx_hundred_code_h");
			runTest("create_idx_tenpct_name_h");
			runTest("create_idx_updates_code_h");
			runTest("create_idx_tenpct_code_h");
			runTest("create_idx_updates_double_bt");
			runTest("create_idx_hundred_foreign");
			
			DatabaseDisconnect();
		}

		public void single_user_tests() 
		{
			long		clocks;
			TimeSpan	elapsed;

			DatabaseConnect();
			
			clocks = System.DateTime.Now.Ticks;

			runTest("sel_1_cl");
			runTest("join_3_cl");
			runTest("sel_100_ncl");
			runTest("table_scan");
			runTest("agg_func");
			runTest("agg_scal");
			runTest("sel_100_cl");
			runTest("join_3_ncl");
			runTest("sel_10pct_ncl");
			runTest("agg_simple_report");
			runTest("agg_info_retrieval");
			runTest("agg_create_view");
			runTest("agg_subtotal_report");
			runTest("agg_total_report");
			runTest("join_2_cl");
			runTest("join_2");
			runTest("sel_variable_select_low");
			runTest("sel_variable_select_high");
			runTest("join_4_cl");
			runTest("proj_100");
			runTest("join_4_ncl");
			runTest("proj_10pct");
			runTest("sel_1_ncl");
			runTest("join_2_ncl");
			runTest("integrity_test");
			runTest("drop_updates_keys");
			runTest("bulk_save");
			runTest("bulk_modify");
			runTest("upd_append_duplicate");
			runTest("upd_remove_duplicate");
			runTest("upd_app_t_mid");
			runTest("upd_mod_t_mid");
			runTest("upd_del_t_mid");
			runTest("upd_app_t_end");
			runTest("upd_mod_t_end");
			runTest("upd_del_t_end");
			runTest("create_idx_updates_code_h");
			runTest("upd_app_t_mid");
			runTest("upd_mod_t_cod");
			runTest("upd_del_t_mid");
			runTest("create_idx_updates_int_bt");
			runTest("upd_app_t_mid");
			runTest("upd_mod_t_int");
			runTest("upd_del_t_mid");
			runTest("bulk_append");
			runTest("bulk_delete");

			elapsed = new TimeSpan(DateTime.Now.Ticks - clocks);

			if (log != null) log.Simple("\r\nSingle user test ( {0} )\r\n\r\n",
								 elapsed.ToString());

			DatabaseDisconnect();
		}

		public void multi_user_tests(int nInstances) 
		{	
			TimeSpan	fTime;
			long		sTime;			

			userProcess = new Thread[nInstances];
			
			if (log != null) log.Simple("\"Executing multi-user tests with {0} user task{1}\"\r\n\r\n",
										   nInstances, ((nInstances != 1) ? "s" : ""));

			/* Step 1 -- Backup updates relation, including indices, 
			 * to tape or other device. This is done early on.
			 */
			
			
			
			/* Step 2 -- Run IR (Mix 1) test for 15 minutes.	*/
			if (Progress != null)
			{
				Progress(this, 
					new ProgressMessageEventArgs("Run IR (Mix 1) test for 15 minutes (" + DateTime.Now.ToString() + ")."));
			}

			iters		= 0;
			timeToRun	= 15;
					
			for (int i = 0; i < nInstances; i++) 
			{
				userProcess[i]		= new Thread(new ThreadStart(ir_select));
				userProcess[i].Name = "User " + i.ToString();
				userProcess[i].Start();
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				userProcess[i].Join();
			}
									
			
			/* Step 3 -- Measure throughput in IR test for five minutes.	*/
			if (Progress != null)
			{
				Progress(this, 
					new ProgressMessageEventArgs("Run Measure throughput in IR test for five minutes (" + DateTime.Now.ToString() + ")."));
			}
			
			iters		= 0;
			sTime		= DateTime.Now.Ticks;
			timeToRun	= 5;
						
			for (int i = 0; i < nInstances; i++)
			{
				userProcess[i] = new Thread(new ThreadStart(ir_select));
				userProcess[i].Name = "User " + i.ToString();
				userProcess[i].Start();
			}
						
			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				userProcess[i].Join();
			}

			fTime = new TimeSpan(DateTime.Now.Ticks - sTime);
			if (log != null) log.Simple("Mixed IR (tup/sec)\t{0}"			+
										   "\treturned in {1} minutes"			,
										   Math.Round((double)iters/fTime.TotalSeconds, 4)	, 
										   fTime.ToString());

			
			/* Step 4 -- A Mixed Workload IR Test, where one user executes a cross
			 * section of ten update and retrieval queries, and all the others 
			 * execute the same IR query as in the second test.
			 */
			if (Progress != null)
			{
				Progress(this, 
					new ProgressMessageEventArgs("Run Mixed Workload IR test (Mix 3) (" + DateTime.Now.ToString() + ")."));
			}
			
			userProcess[0]		= new Thread(new ThreadStart(cross_section_tests));
			userProcess[0].Name = "User " + 0.ToString();
			userProcess[0].Start();			
			timeToRun	= -1;	// Exec the only one time in each thread
			for (int i = 1; i < nInstances; i++) 
			{
				userProcess[i] = new Thread(new ThreadStart(ir_select));
				userProcess[i].Name = "User " + i.ToString();
				userProcess[i].Start();
			}
						
			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				userProcess[i].Join();
			}
						
			
			/* Step 5 -- Run queries to check correctness of the sequential
			 * and random bulk updates.
			 */
			if (Progress != null)
			{
				Progress(this, 
					new ProgressMessageEventArgs("Check correctness of the sequential and random bulk updates (" + DateTime.Now.ToString() + ")."));
			}
			
			DatabaseConnect();
			runTest("mu_checkmod_100_seq");
			runTest("mu_checkmod_100_rand");
			DatabaseDisconnect();

			
			/* Step 6 - Recover updates relation from backup tape (Step 1)
			 * and log (from Steps 2, 3, 4, and 5).	
			 */


			/* Step 7 - Perform correctness checks, checkmod_100_seq and 
			 * checkmod_100_rand. Remove temporary tables: sel100seq and 
			 * sel100rand.
			 */
			if (Progress != null)
			{
				Progress(this, 
					new ProgressMessageEventArgs("Check correctness of the sequential and random bulk updates (" + DateTime.Now.ToString() + ")."));
			}
			
			DatabaseConnect();
			runTest("mu_checkmod_100_seq");
			runTest("mu_checkmod_100_rand");

			runTest("mu_drop_sel100_seq");
			runTest("mu_drop_sel100_rand");
			DatabaseDisconnect();

			
			/* Step 8 - Run OLTP test for 15 minutes.	*/
			if (Progress != null)
			{
				Progress(this, 
					new ProgressMessageEventArgs("Run OLTP test for 15 minutes (" + DateTime.Now.ToString() + ")."));
			}
			
			timeToRun = 15;						
			for (int i = 0; i < nInstances; i++) 
			{
				userProcess[i] = new Thread(new ThreadStart(oltp_update));
				userProcess[i].Name = "User " + i.ToString();
				userProcess[i].Start();
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				userProcess[i].Join();
			}
			
			/* Step 9 -- Measure throughput in IR test for five minutes.	*/
			if (Progress != null)
			{
				Progress(this, 
					new ProgressMessageEventArgs("Run Measure throughput in IR test for five minutes (" + DateTime.Now.ToString() + ")."));
			}

			iters		= 0;
			sTime		= DateTime.Now.Ticks;
			timeToRun	= 5;						
			for (int i = 0; i < nInstances; i++)
			{
				userProcess[i] = new Thread(new ThreadStart(ir_select));
				userProcess[i].Name = "User " + i.ToString();
				userProcess[i].Start();
			}
			
			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				userProcess[i].Join();
			}
						
			fTime = new TimeSpan(DateTime.Now.Ticks - sTime);
			if (log != null) log.Simple("Mixed OLTP (tup/sec)\t{0}"			+
										   "\treturned in {1} minutes\n"		,
										   Math.Round((double)iters/fTime.TotalSeconds, 4)	, 
										   fTime.ToString());

			/* Step 10 -- Replace one background OLTP script with the cross 
			 * section script. This is the Mixed Workload OLTP test (Mix 4). 
			 * This step is variable length.
			 */
			if (Progress != null)
			{
				Progress(this, 
					new ProgressMessageEventArgs("Run Mixed Workload OLTP test (Mix 4) (" + DateTime.Now.ToString() + ")."));
			}
						
			userProcess[0]		= new Thread(new ThreadStart(cross_section_tests));
			userProcess[0].Name = "User " + 0.ToString();
			userProcess[0].Start();
			timeToRun	= -1;	// Exec the only one time in each thread
			for (int i = 1; i < nInstances; i++)
			{
				userProcess[i] = new Thread(new ThreadStart(oltp_update));
				userProcess[i].Name = "User " + i.ToString();
				userProcess[i].Start();
			}			

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				userProcess[i].Join();
			}
									
			/* Step 11 -- Perform correctness checks, checkmod_100_seq and 
			 * checkmod_100_rand. Remove temporary tables: sel100seq and 
			 * sel100rand.
			 */
			if (Progress != null)
			{
				Progress(this, 
					new ProgressMessageEventArgs("Check correctness of the sequential and random bulk updates (" + DateTime.Now.ToString() + ")."));
			}
			
			DatabaseConnect();
			runTest("mu_checkmod_100_seq");			
			runTest("mu_checkmod_100_rand");
			
			runTest("mu_drop_sel100_seq");
			runTest("mu_drop_sel100_rand");
			DatabaseDisconnect();
		}

		private void cross_section_tests() 
		{
			long	startTime;
			long	endTime;

			DatabaseConnect();

			startTime = DateTime.Now.Ticks;

			runTest("o_mode_tiny");
			runTest("o_mode_100k");
			runTest("sel_1_ncl");
			runTest("sel_1_ncl");
			runTest("sel_1_ncl");
			runTest("agg_simple_report");
			runTest("mu_sel_100_seq");
			runTest("mu_sel_100_rand");
			runTest("mu_mod_100_seq");
			runTest("mu_mod_100_rand");
			runTest("mu_unmod_100_seq");
			runTest("mu_unmod_100_rand");

			endTime = DateTime.Now.Ticks;
			TimeSpan elapsed = new TimeSpan(endTime - startTime);

			DatabaseDisconnect();

			if (log != null) log.Simple("CrossSectionTests \t( {1} )", elapsed.ToString());
		}

		private void ir_select()
		{		
			ITestSuite	testSuite = TestSuiteFactory.GetTestSuite(testSuiteName, configuration);

			testSuite.DatabaseConnect();
			
			DateTime	endTime	= DateTime.Now;

			if (timeToRun > 0)
			{
				endTime = DateTime.Now.AddMinutes(timeToRun);
				while (endTime >= DateTime.Now)
				{					
					testSuite.mu_ir_select();
					iters++;
				}
			}
			else
			{
				mu_ir_select();
			}
			
			testSuite.DatabaseDisconnect();
		}
		
		private void oltp_update()
		{			
			ITestSuite	testSuite = TestSuiteFactory.GetTestSuite(testSuiteName, configuration);
			
			testSuite.DatabaseConnect();
			
			DateTime	endTime	= DateTime.Now;

			if (timeToRun > 0)
			{
				endTime = DateTime.Now.AddMinutes(timeToRun);
				while (endTime >= DateTime.Now)
				{
					testSuite.mu_oltp_update();
				}
			}
			else
			{
				testSuite.mu_oltp_update();
			}
			
			testSuite.DatabaseDisconnect();
		}

		private void runTest(string testName)
		{
			Type 		type	= null;
			MethodInfo	method	= null;
			long		clocks	= 0;

			testFailed	= false;

			type	= this.GetType();
			method 	= type.GetMethod(testName, BindingFlags.Public|BindingFlags.NonPublic|BindingFlags.Instance);
			
			// Set IsolationLevel for test execution
			set_isolation_level(testName);

			// Reset TestFailed property value
			testFailed = false;

			clocks = DateTime.Now.Ticks;

			method.Invoke(this, null);

			clocks	= DateTime.Now.Ticks - clocks;

			testName = formatTestName(testName);

			TimeSpan elapsed = new TimeSpan(clocks);				

			if (Result != null)
			{
				Result(this, new TestResultEventArgs(testName, testResult, elapsed, testFailed));
			}

			StringBuilder logMessage = new StringBuilder();

			if (testFailed)
			{
				if (log != null) log.Simple("-----> {0}\tfailed <-----", testName);
			}
			else
			{
				logMessage.AppendFormat(
					"{0} ( {1} )\treturn value = {2} \t\t"	,
					testName								,
					elapsed.ToString(),
					testResult);
			}

			if (log != null) log.Simple(logMessage.ToString());

			elapsed = new TimeSpan(DateTime.Now.Ticks - clocks);
		}

		private string formatTestName(string methodName)
		{
			int length = 30 - methodName.Length;

			for (int i = 0; i < length; i++)
			{
				methodName = " " + methodName;
			}

			return methodName;
		}

		#endregion
		
		#region DATABASE_METHODS

		private void createIndex(IndexType indextype, string indexName, string tableName, string fields)
		{
			string	createIndexStmt = String.Empty;
			
			switch (indextype)
			{
				case IndexType.Btree:
					createIndexStmt = configuration.BtreeIndexStmt;
					break;
				
				case IndexType.Clustered:
					createIndexStmt = configuration.ClusteredIndexStmt;
					break;

				case IndexType.Hash:
					createIndexStmt = configuration.HashIndexStmt;
					break;
			}

			createIndexStmt	= createIndexStmt.Replace("@INDEX_NAME", indexName);
			createIndexStmt	= createIndexStmt.Replace("@TABLE_NAME", tableName);
			createIndexStmt	= createIndexStmt.Replace("@INDEX_FIELDS", fields);

			try
			{
				beginTransaction();
				executeStatement(createIndexStmt);
				commitTransaction();
			}
			catch(Exception ex)
			{
				if (log != null) log.Error("btree error {0}", ex.Message);
				throw ex;				
			}
		}

		private void createForeignKey(string foreignTable, string constraintName, 
										string foreignKeyColumns,
										string referencesTableName, 
										string referencesFields)
		{
			StringBuilder commandText = new StringBuilder();

			commandText.AppendFormat("alter table {0} add constraint {1} foreign key ({2}) references {3} ({4}) {5} {6}",
									foreignTable, constraintName, foreignKeyColumns, 
									referencesTableName, referencesFields,
									"on delete cascade", "on update cascade");

			try
			{
				beginTransaction();
				executeStatement(commandText.ToString());
				commitTransaction();
			}
			catch(Exception ex)
			{
				if (log != null) log.Error("foreign key error {0}", ex.Message);
				throw ex;
			}
		}

		private void createTable(string tableName, string tableStructure, string primaryKey) 
		{
			try
			{
				StringBuilder commandText = new StringBuilder();

				if (primaryKey != null)
				{
					commandText.AppendFormat(
						"create table {0} ({1}, primary key ({2}))", tableName, tableStructure, primaryKey);
				}
				else
				{
					commandText.AppendFormat(
						"create table {0} ({1})", tableName, tableStructure);
				}

				beginTransaction();
				executeStatement(commandText.ToString());
				commitTransaction();
			}
			catch (Exception ex)
			{
				if (log != null) log.Error("error create table {0}", ex.Message);
				throw ex;
			}
		}

		protected void cursorOpen(string commandText)
		{
			try
			{
				cmdCursor	= getCommand(commandText);
				cursor		= cmdCursor.ExecuteReader();
			}
			catch (Exception ex)
			{
				if (log != null) log.Error("cursorOpen failed {0}", ex.Message);

				if (cursor != null)
				{
					cursor.Close();
					cursor = null;
				}

				if (cmdCursor != null)
				{
					cmdCursor.Dispose();
					cmdCursor = null;
				}
				
				throw ex;
			}
		}

		protected bool cursorFetch()
		{
			bool fetched = false;

			try
			{
				fetched = cursor.Read();
			}
			catch (Exception ex)
			{
				if (log != null) log.Error("cursorFetch failed {0}", ex.Message);
			}

			return fetched;
		}
		
		protected void cursorClose()
		{
			try
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			catch(Exception ex)
			{				
				if (log != null) log.Error("cursorClose failed {0}", ex.Message);

				throw ex;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
					cursor = null;
				}

				if (cmdCursor != null)
				{
					cmdCursor.Dispose();
					cmdCursor = null;
				}
			}
		}

		public void DatabaseConnect()
		{
			try
			{
				connection = dataHelper.CreateConnection(configuration.ConnectionString);
				connection.Open();
			}
			catch (Exception ex)
			{
				if (log != null) log.Error("DatabaseConnect error {0}", ex.Message);
				throw ex;
			}
		}

		private void databaseCreate()
		{
			string	dataSource	= "localhost";
			int		port		= 3050;
			string	database	= @"c:\asp3ap.fdb";
			string	user		= "SYSDBA";
			string	password	= "masterkey";
			byte	dialect		= 3;
			bool	forcedWrite = configuration.ForcedWrites;
			short	pageSize	= 4096;
			string	charset		= "NONE";
			bool	ssl			= false;
			int		serverType	= 0;

			Regex			search	 = new Regex(@"([\w\s\d]*)\s*=\s*([^;]*)");
			MatchCollection	elements = search.Matches(this.configuration.ConnectionString);

			foreach (Match element in elements)
			{
				switch (element.Groups[1].Value.Trim().ToLower())
				{
					case "datasource":
					case "server":
					case "host":
						dataSource = element.Groups[2].Value.Trim();
						break;

					case "database":
						database = element.Groups[2].Value.Trim();
						break;

					case "user name":
					case "user":
					case "user id":
					case "userid":
						user = element.Groups[2].Value.Trim();
						break;

					case "user password":
					case "password":
						password = element.Groups[2].Value.Trim();
						break;

					case "port":
						port = Int32.Parse(element.Groups[2].Value.Trim());
						break;

					case "dialect":
						dialect = byte.Parse(element.Groups[2].Value.Trim());
						break;

					case "ssl":
						ssl = Boolean.Parse(element.Groups[2].Value.Trim());
						break;

					case "charset":
						charset = element.Groups[2].Value.Trim();
						break;

					case "servertype":
					case "server type":
						serverType = Int32.Parse(element.Groups[2].Value.Trim());
						break;
				}
			}

			Hashtable values = new Hashtable();

			values.Add("DataSource"	, dataSource);
			values.Add("Port"		, port);
			values.Add("Database"	, database);
			values.Add("User"		, user);
			values.Add("Password"	, password);
			values.Add("Dialect"	, dialect);
			values.Add("ForcedWrites", forcedWrite);
			values.Add("PageSize"	, pageSize);
			values.Add("Charset"	, charset);
			values.Add("ServerType"	, serverType);
			values.Add("SSL"		, ssl);

			dataHelper.CreateDatabase(values);
		}

		public void DatabaseDisconnect()
		{
			try
			{
				if (connection != null)
				{
					connection.Close();
					
					connection	= null;
					transaction = null;
				}
			}
			catch (Exception ex)
			{
				if (log != null) log.Error("disconnect error {0}", ex.Message);
				throw ex;
			}
		}

		protected void executeStatement(string commandText)
		{			
			IDbCommand command = null;

			try
			{
				command = getCommand(commandText);
				command.ExecuteNonQuery();
			}
			catch (Exception ex)
			{
				rollbackTransaction();
				
				throw ex;
			}
			finally
			{	
				if (command != null)
				{
					command.Dispose();
					command = null;	
				}
			}
		}

		protected void beginTransaction()
		{
			try
			{
				transaction = connection.BeginTransaction(isolation);
			}
			catch(Exception ex)
			{
				if (log != null) log.Error("beginTransaction failed {0}", ex.Message);
				throw ex;
			}
		}

		protected void commitTransaction()
		{
			try
			{
				transaction.Commit();
			}
			catch (Exception ex)
			{					
				if (log != null) log.Error("Commit failed {0}", ex.Message);
				throw ex;
			}
		}

		protected void rollbackTransaction()
		{
			try
			{
				
				transaction.Rollback();
			}
			catch (Exception ex)
			{
				if (log != null) log.Error("Rollback failed {0}", ex.Message);
				throw ex;
			}
		}

		public void load_data()
		{
			try
			{
				DateTime start = DateTime.Now;

				loadTinyFile("tiny");

				if (Result != null)
				{
					Result(this, 
						new TestResultEventArgs(
							"	tiny file loaded", 
							0, 
							DateTime.Now - start, false));
				}

				start = DateTime.Now;
								
				loadFile("uniques");

				if (Result != null)
				{
					Result(this, 
						new TestResultEventArgs(
						"	uniques file loaded", 
						0, 
						DateTime.Now - start, false));
				}

				start = DateTime.Now;
				
				loadFile("updates");

				if (Result != null)
				{
					Result(this, 
						new TestResultEventArgs(
						"	updates file loaded", 
						0, 
						DateTime.Now - start, false));
				}

				start = DateTime.Now;
				
				loadFile("hundred");

				if (Result != null)
				{
					Result(this, 
						new TestResultEventArgs(
						"	hundred file loaded", 
						0, 
						DateTime.Now - start, false));
				}

				start = DateTime.Now;
				
				loadFile("tenpct");

				if (Result != null)
				{
					Result(this, 
						new TestResultEventArgs(
						"	tenpct file loaded", 
						0, 
						DateTime.Now - start, false));
				}
			}
			catch (Exception ex)
			{
				if (log != null) log.Error("load failed {0}", ex.Message);

				rollbackTransaction();

				throw ex;
			}
		}

		private void loadFile(string table)
		{
			StringBuilder	commandText = new StringBuilder();
			StreamReader	stream		= null;
			IDbCommand		command		= null;
			
			commandText.AppendFormat("insert into {0} values (@col_key,@col_int,@col_signed,@col_float,@col_double,@col_decim,@col_date,@col_code,@col_name,@col_address)", table);

			/* Crate command */
			command = getCommand(commandText.ToString());
			
			/* Add parameters	*/
			command.Parameters.Add(dataHelper.CreateParameter("@col_key", null));
			command.Parameters.Add(dataHelper.CreateParameter("@col_int", null));
			command.Parameters.Add(dataHelper.CreateParameter("@col_signed", null));
			command.Parameters.Add(dataHelper.CreateParameter("@col_float", null));
			command.Parameters.Add(dataHelper.CreateParameter("@col_double", null));
			command.Parameters.Add(dataHelper.CreateParameter("@col_decim", null));
			command.Parameters.Add(dataHelper.CreateParameter("@col_date", null));
			command.Parameters.Add(dataHelper.CreateParameter("@col_code", null));
			command.Parameters.Add(dataHelper.CreateParameter("@col_name", null));
			command.Parameters.Add(dataHelper.CreateParameter("@col_address", null));

			string path = Path.GetFullPath(configuration.DataPath);
			if (!path.EndsWith(Path.DirectorySeparatorChar.ToString()))
			{
				path += Path.DirectorySeparatorChar;
			}

			if (!File.Exists(path + "asap." + table))
			{
				throw new FileNotFoundException(
					"AS3AP data file not found",
					path + "asap." + table);
			}

			stream = new StreamReader(
				(System.IO.Stream)File.Open(
				path + "asap." + table	,
				FileMode.Open			,
				FileAccess.Read			,
				FileShare.None));

			int		rowCount			= 0;
			bool	transactionPending	= false;
			bool	commandPrepared		= false;
			while (stream.Peek() > -1)
			{
				if (rowCount == 0)
				{
					beginTransaction();
					transactionPending	= true;
					command.Transaction = this.transaction;

					/* Prepare command execution	*/
					if (!commandPrepared)
					{
						command.Prepare();
						commandPrepared = true;
					}
				}

				string[] elements = stream.ReadLine().Split(',');
			
				for (int i = 0; i < 10; i++)
				{
					((IDataParameter)command.Parameters[i]).Value = elements[i];
				}

				command.ExecuteNonQuery();

				rowCount++;

				if (rowCount >= 1000)
				{
					commitTransaction();
					transactionPending	= false;
					rowCount			= 0;
				}
			}

			if (transactionPending)
			{
				commitTransaction();
			}

			command.Dispose();
			stream.Close();
		}

		private void loadTinyFile(string table)
		{
			beginTransaction();

			StringBuilder	commandText = new StringBuilder();
			StreamReader	stream		= null;
			IDbCommand		command		= null;

			commandText.AppendFormat("insert into {0} values (@col_key)", table);

			/* Crate command */
			command = getCommand(commandText.ToString());

			/* Add parameters	*/
			command.Parameters.Add(dataHelper.CreateParameter("@col_key", null));

			/* Prepare command execution	*/
			command.Prepare();

			string path = Path.GetFullPath(configuration.DataPath);
			if (!path.EndsWith(Path.DirectorySeparatorChar.ToString()))
			{
				path += Path.DirectorySeparatorChar;
			}

			if (!File.Exists(path + "asap." + table))
			{
				throw new FileNotFoundException(
					"AS3AP data file not found",
					path + "asap." + table);
			}

			stream = new StreamReader(
				(System.IO.Stream)File.Open(
				path + "asap." + table	,
				FileMode.Open			,
				FileAccess.Read			,
				FileShare.None));

			while (stream.Peek() > -1)
			{
				string[] elements = stream.ReadLine().Split(',');
			
				((IDataParameter)command.Parameters[0]).Value = elements[0];
	
				command.ExecuteNonQuery();
			}

			command.Dispose();
			stream.Close();

			commitTransaction();
		}

		protected IDbCommand getCommand(string commandText)
		{
			IDbCommand command = connection.CreateCommand();

			command.CommandText = commandText;
			command.Transaction	= transaction;

			return command;
		}

		#endregion	
	}
}
