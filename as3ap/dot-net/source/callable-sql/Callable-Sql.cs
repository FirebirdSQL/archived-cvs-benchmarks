//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
//
// Ported from OSDB project at http://osdb.sourceforge.net
//
// Author: Carlos Guzmán Álvarez <carlosga@telefonica.net>
//
// Distributable under GPL license.
// You may obtain a copy of the License at http://www.gnu.org/copyleft/gpl.html
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GPL License for more details.
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
	public class CallableSql
	{
		#region FIELDS

		private IBackend	backend;
		private bool		doIndexes;
		private bool		testFailed = false;

		private int			tupleCount = 0;

		#endregion

		#region PROPERTIES

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

		public CallableSql(string backendName)
		{
			Assembly aseembly = Assembly.Load("AS3AP.BenchMark.Backends");

			this.doIndexes	= true;			

			this.backend	= (IBackend)Activator.CreateInstance(
											aseembly.GetType(backendName));
		}

		#endregion

		#region METHODS

		public void setup_database()
		{
			try
			{
				// Remove reportview
				backend.ddl("drop view reportview");

				// Remove saveupdates table
				backend.ddl("drop table saveupdates");

				// Create indexes for updates table				
				create_idx_updates_double_bt();
				create_idx_updates_decim_bt();
			}
			catch (Exception)
			{
			}
		}

		public int load(long dataSize)
		{
			int returnValue = 0;

			try
			{
				returnValue = backend.CreateData(dataSize);
			}
			catch(Exception)
			{
				testFailed = true;
			}

			return returnValue;
		}

		public int agg_create_view() 
		{
			try
			{
				backend.ddl("create view "									+
					"reportview(col_key,col_signed,col_date,col_decim, "	+
								"col_name,col_code,col_int) as "			+
					"select updates.col_key, updates.col_signed, "			+
							"updates.col_date, updates.col_decim, "			+
							"hundred.col_name, hundred.col_code, "			+
							"hundred.col_int "								+
						"from updates, hundred "							+
						"where updates.col_key = hundred.col_key");
			}
			catch(Exception)
			{
				testFailed = true;
				return -1;
			}

			return 0; 
		}


		public int agg_func() 
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
				testFailed = true;
				return -1;
			}
			finally
			{
				backend.TransactionCommit();
				backend.CursorClose();
			}

			return count; 
		}


		public int agg_info_retrieval() 
		{
			int tupleReturned;

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

				tupleReturned = backend.Cursor.GetInt32(0);
			}
			catch(Exception)
			{
				testFailed = true;
				return -1;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return tupleReturned;
		}


		public int agg_scal() 
		{	
			int tupleReturned;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen("select min(col_key) from uniques");
				
				backend.CursorFetch();
				
				tupleReturned = backend.Cursor.GetInt32(0);
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return tupleReturned; 
		}


		public int agg_simple_report() 
		{
			int tupleReturned;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select avg(updates.col_decim) "						+
						"from updates "										+
					"where updates.col_key in "								+
						"(select updates.col_key "							+
							"from updates, hundred "						+
								"where hundred.col_key = updates.col_key "	+
								"and updates.col_decim > 980000000)");
				
				backend.CursorFetch();

				tupleReturned = backend.Cursor.GetInt32(0);
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return tupleReturned; 
		}


		public int agg_subtotal_report() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select avg(col_signed), min(col_signed), max(col_signed), "	+
					"max(col_date), min(col_date), "						+
					"count(distinct col_name), count(col_name), "			+
					"col_code, col_int "									+
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
				testFailed = true;
				return -1;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return count;
		}


		public int agg_total_report() 
		{
			int tupleReturned;

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

				backend.CursorFetch();

				tupleReturned = backend.Cursor.GetInt32(0);
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return tupleReturned;
		}


		public int bulk_append() 
		{
			try
			{
				backend.TransactionBegin();
				backend.dml("insert into updates select * from saveupdates");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}

			return 0;
		}


		public int bulk_delete() 
		{
			try
			{
				backend.TransactionBegin();
				backend.dml("delete from updates where col_key < 0");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}

			return 0;
		}


		public int bulk_modify() 
		{
			try
			{
				backend.TransactionBegin();
				backend.dml("update updates "				+
					"set col_key = col_key - 100000 "		+
					"where col_key between 5000 and 5999");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}

			return 0;
		}


		public int bulk_save()
		{
			try
			{
				create_table_standard("saveupdates", null);

				backend.TransactionBegin();
				backend.dml("insert into saveupdates select * "	+
							"from updates where col_key between 5000 and 5999");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}

			return 0;
		}


		public int create_idx_hundred_code_h() 
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

			return 0;
		}


		public int create_idx_hundred_foreign() 
		{
			if (doIndexes) 
			{  
				try
				{
					backend.CreateIndexForeign("hundred"				, 
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

			return 0;
		}


		public int create_idx_hundred_key_bt() 
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

			return 0;
		}


		public int create_idx_tenpct_code_h() 
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

			return 0;
		}


		public int create_idx_tenpct_decim_bt() 
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

			return 0;
		}


		public int create_idx_tenpct_double_bt() 
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

			return 0; 
		}


		public int create_idx_tenpct_float_bt() 
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

			return 0;
		}


		public int create_idx_tenpct_int_bt() 
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

			return 0;
		}

		public int create_idx_tenpct_key_bt() 
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

			return 0;
		}


		public int create_idx_tenpct_key_code_bt() 
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

			return 0;
		}


		public int create_idx_tenpct_name_h() 
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
			
			return 0;
		}


		public int create_idx_tenpct_signed_bt() 
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

			return 0;
		}


		public int create_idx_tiny_key_bt() 
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

			return 0;
		}


		public int create_idx_uniques_code_h() 
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

			return 0;
		} 


		public int create_idx_uniques_key_bt() 
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

			return 0;
		} 


		public int create_idx_updates_code_h() 
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

			return 0;
		}


		public int create_idx_updates_decim_bt() 
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

			return 0;
		}


		public int create_idx_updates_double_bt() 
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
			return 0;
		} 


		public int create_idx_updates_int_bt() 
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

			return 0;
		} 


		public int create_idx_updates_key_bt() 
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

			return 0;
		}


		public void create_table_standard(string tName, string tPrimaryKey)
		{
			StringBuilder cmdBuf = new StringBuilder();

			cmdBuf.AppendFormat("create table {0}( "						+
								"col_key     int             not null, "	+
								"col_int     int             not null, "	+
								"col_signed  int             not null, "	+
								"col_float   float           not null, "	+
								"col_double  float           not null, "	+
								"col_decim   numeric(18,2)   not null, "	+
								"col_date    char(20)        not null, "	+
								"col_code    char(10)        not null, "	+
								"col_name    char(20)        not null, "	+
								"col_address varchar(80)     not null", tName);

			if (tPrimaryKey != null)
			{
				cmdBuf.AppendFormat(",\n primary key ({0}))", tPrimaryKey);
			}
			else
			{
				cmdBuf.Append(")");
			}

			try
			{
				backend.CreateTable(cmdBuf.ToString());
			}
			catch (Exception)
			{
				testFailed = true;
			}
		}

		public int create_tables() 
		{
			try
			{
				create_table_standard("uniques", "col_key");
				create_table_standard("hundred", "col_key");
				create_table_standard("tenpct" , "col_key,col_code");
				create_table_standard("updates", "col_key");

				backend.CreateTable( 
					"create table tiny("			+
						"col_key int not null, "	+
						"primary key (col_key))");
			}
			catch (Exception)
			{
				testFailed = true;
			}

			return 0;
		}


		public int drop_updates_keys() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.ddl("drop index updates_int_bt");
					backend.ddl("drop index updates_double_bt");
					backend.ddl("drop index updates_decim_bt");
					backend.ddl("drop index updates_code_h");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			return 0;
		}


		public int join_2() 
		{
			int	count = 0;

			try
			{			
				backend.TransactionBegin();
				backend.CursorOpen( 
					"select uniques.col_signed, uniques.col_name, "		+
					"hundred.col_signed, hundred.col_name "				+
					"from uniques, hundred "							+
					"where uniques.col_address = hundred.col_address "	+
					"and uniques.col_address = 'SILICON VALLEY'"	);
				
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

			return count;
		}


		public int join_2_cl() 
		{
			int	count = 0;
			
			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select uniques.col_signed, uniques.col_name, "	+
					"hundred.col_signed, hundred.col_name "			+
					"from uniques, hundred "						+
					"where uniques.col_key = hundred.col_key "		+
					"and uniques.col_key = 1000");
				
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

			return count;
		}


		public int integrity_test() 
		{
			try
			{
				create_table_standard("integrity_temp", null);

				backend.TransactionBegin();
				backend.dml("insert into integrity_temp select * from hundred where col_int = 0");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}

			try
			{
				backend.TransactionBegin();
				backend.dml("update hundred set col_signed = '-500000000' where col_int = 0");
				backend.TransactionCommit();

				testFailed = true;				
				return 0;	
			}
			catch (Exception)
			{				
			}

			try
			{
				backend.TransactionBegin();
				backend.dml("delete from hundred where col_int = 0");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}

			try
			{
				backend.TransactionBegin();
				backend.dml("insert into hundred select * from integrity_temp");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}

			try
			{
				backend.ddl("drop table integrity_temp");
			}
			catch (Exception)
			{
				testFailed = true;
				return -1;
			}

			return 0;
		}


		public int join_2_ncl() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen( 
					"select uniques.col_signed, uniques.col_name, "		+
					"hundred.col_signed, hundred.col_name "		+
					"from uniques, hundred "						+
					"where uniques.col_code = hundred.col_code "	+
					"and uniques.col_code = 'BENCHMARKS'"			);
				
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

			return count;
		}


		public int join_3_cl() 
		{
			int	count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select uniques.col_signed, uniques.col_date, "		+
					"hundred.col_signed, hundred.col_date, "	+
					"tenpct.col_signed, tenpct.col_date "		+
					"from uniques, hundred, tenpct "					+
					"where uniques.col_key = hundred.col_key "			+
					"and uniques.col_key = tenpct.col_key "				+
					"and uniques.col_key = 1000");
				
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

			return count;
		}


		public int join_3_ncl() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen( 
					"select uniques.col_signed, uniques.col_date, "		+
					"hundred.col_signed, hundred.col_date, "	+
					"tenpct.col_signed, tenpct.col_date "		+
					"from uniques, hundred, tenpct "					+
					"where uniques.col_code = hundred.col_code "		+
					"and uniques.col_code = tenpct.col_code "			+
					"and uniques.col_code = 'BENCHMARKS'");
				
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

			return count;
		}


		public int join_4_cl() 
		{
			int count = 0;
	
			try
			{
				backend.TransactionBegin();
				backend.CursorOpen(
					"select uniques.col_date, hundred.col_date, "	+
					"tenpct.col_date, updates.col_date "			+
					"from uniques, hundred, tenpct, updates "		+
					"where uniques.col_key = hundred.col_key "		+
					"and uniques.col_key = tenpct.col_key "			+
					"and uniques.col_key = updates.col_key "		+
					"and uniques.col_key = 1000");
				
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

			return count;
		}


		public int join_4_ncl() 
		{
			int count=0;

			try
			{
				backend.TransactionBegin();
				backend.CursorOpen( 
					"select uniques.col_date, hundred.col_date, "		+
					"tenpct.col_date, updates.col_date "	+
					"from uniques, hundred, tenpct, updates "		+
					"where uniques.col_code = hundred.col_code "	+
					"and uniques.col_code = tenpct.col_code "	+
					"and uniques.col_code = updates.col_code "	+
					"and uniques.col_code = 'BENCHMARKS'");
				
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

			return count;
		}


		public int o_mode_tiny(IsolationLevel isolationLevel)
		{	
			int count = 0;

			lock (backend)
			{
				try
				{
					backend.Isolation = isolationLevel;

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
					backend.Isolation = IsolationLevel.ReadCommitted;
				}
			}

			return count;
		}


		public int o_mode_100k(IsolationLevel isolationLevel)
		{
			int count = 0;

			lock (backend)
			{
				try
				{
					backend.Isolation = isolationLevel;

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
					backend.Isolation = IsolationLevel.ReadCommitted;
				}
			}

			return count;
		}


		public int mu_checkmod_100_rand() 
		{
			int count = 0;
			
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

					count = backend.Cursor.GetInt32(0);
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

			if (count != 100) 
			{
				testFailed = true;
			}

			return count;
		}

		public int mu_drop_sel100_rand() 
		{
			lock (backend)
			{
				try
				{
					backend.ddl("drop table sel100rand");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			return 0;
		}

		public int mu_checkmod_100_seq() 
		{
			int count = 0;

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
				
					count = backend.Cursor.GetInt32(0); 
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

			if (count != 100) 
			{
				testFailed = true;
			}

			return count;
		}


		public int mu_drop_sel100_seq() 
		{
			lock (backend)
			{
				try
				{
					backend.ddl("drop table sel100seq");
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			return 0;
		}


		public int mu_ir_select()
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

			return count;
		}


		public int mu_mod_100_rand(IsolationLevel isolationLevel) 
		{
			int count = 0;

			lock (backend)
			{
				try
				{
					backend.Isolation = isolationLevel;

					backend.TransactionBegin();
					backend.dml("update updates "				+
						"set col_double=col_double+100000000 "	+
						"where col_int between 1001 and 1100");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{
					backend.Isolation = IsolationLevel.ReadCommitted;
				}
			}

			return count;
		}


		public int mu_mod_100_seq(IsolationLevel isolationLevel) 
		{
			lock (backend)
			{
				try
				{
					backend.Isolation = isolationLevel;

					backend.TransactionBegin();					
					backend.dml("update updates "					+
						"set col_double = col_double+100000000 "	+
						"where col_key between 1001 and 1100");
					backend.TransactionRollback();
				}
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{
					backend.Isolation = IsolationLevel.ReadCommitted;
				}
			}

			return 0;
		}


		public int mu_oltp_update()
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
					backend.dml(lineBuf.ToString());
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
			}

			return 0;
		}


		public int mu_sel_100_rand(IsolationLevel isolationLevel) 
		{
			lock (backend)
			{
				try
				{
					backend.Isolation = isolationLevel;

					create_table_standard("sel100rand", null);

					backend.TransactionBegin();
					backend.dml("insert into sel100rand select * from updates "	+
						"where updates.col_int between 1001 and 1100");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{
					backend.Isolation = IsolationLevel.ReadCommitted;
				}
			}

			return 0;
		}


		public int mu_sel_100_seq(IsolationLevel isolationLevel) 
		{
			lock (backend)
			{
				try
				{
					backend.Isolation = isolationLevel;

					create_table_standard("sel100seq", null);

					backend.TransactionBegin();
					backend.dml("insert into sel100seq select * from updates "	+
						"where updates.col_key between 1001 and 1100");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{
					backend.Isolation = IsolationLevel.ReadCommitted;
				}
			}

			return 0;
		}


		public int mu_unmod_100_rand(IsolationLevel isolationLevel) 
		{
			lock (backend)
			{
				try
				{
					backend.Isolation = isolationLevel;

					backend.TransactionBegin();
					backend.dml( "update updates "				+
						"set col_double=col_double-100000000 "	+
						"where col_int between 1001 and 1100");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{
					backend.Isolation = IsolationLevel.ReadCommitted;
				}
			}

			return 0;
		}


		public int mu_unmod_100_seq(IsolationLevel isolationLevel) 
		{
			lock (backend)
			{
				try
				{
					backend.Isolation = isolationLevel;

					backend.TransactionBegin();
					backend.dml("update updates "				+
						"set col_double=col_double-100000000 "	+
						"where col_key between 1001 and 1100");
					backend.TransactionCommit();
				}
				catch (Exception)
				{
					testFailed = true;
				}
				finally
				{
					backend.Isolation = IsolationLevel.ReadCommitted;
				}
			}

			return 0; 
		}


		public int proj_100() 
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
					testFailed = true;
				}
				finally
				{
					backend.CursorClose();
					backend.TransactionCommit();
				}
			}

			return count;
		}


		public int proj_10pct() 
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
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return count;
		}


		public int sel_1_cl() 
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
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return count; 
		}


		public int sel_1_ncl(IsolationLevel isolationLevel) 
		{
			int count = 0;

			try
			{
				backend.Isolation = isolationLevel;

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
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
				backend.Isolation = IsolationLevel.ReadCommitted;
			}

			return count;
		}


		public int sel_100_cl() 
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
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return count;
		}


		public int sel_100_ncl() 
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
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}


			return count;
		}


		public int	sel_10pct_ncl() 
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
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return count;
		}


		public int	sel_variable_select(long foo) 
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
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return count;
		}


		public int sel_variable_select_high() 
		{
			return sel_variable_select(-250000000);
		}


		public int sel_variable_select_low() 
		{
			return sel_variable_select(-500000000);
		}


		public int table_scan() 
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
				testFailed = true;
			}
			finally
			{
				backend.CursorClose();
				backend.TransactionCommit();
			}

			return count;
		}


		public int upd_app_t_end() 
		{
			int count = 0;
		
			try
			{
				backend.TransactionBegin();
				backend.dml("insert into updates "					+
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
				return -1;
			}

			return count; 
		}


		public int	upd_app_t_mid() 
		{
			int count = 0;

			try
			{
				backend.TransactionBegin();
				backend.dml("insert into updates "							+
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
				return -1;
			}

			return count;

		}


		public int upd_append_duplicate() 
		{
			if (doIndexes) 
			{
				try
				{
					backend.TransactionBegin();
					backend.dml( "insert into updates  "				+
						"values (6000, 0, 60000, 39997.90, "			+
								"50005.00, 50005.00, "					+
								"'11/10/1985', 'CONTROLLER', "			+
								"'ALICE IN WONDERLAND', "				+
								"'UNIVERSITY OF ILLINOIS AT CHICAGO')"); 
					backend.TransactionCommit();

					testFailed = true;
				}
				catch (Exception)
				{					
				}
			}

			return 0;
		}


		public int upd_del_t_end() 
		{
			try
			{
				backend.TransactionBegin();
				backend.dml("delete from updates where col_key = -1000"); 
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			return 0; 
		}


		public int upd_del_t_mid() 
		{
			try
			{
				backend.TransactionBegin();
				backend.dml("delete from updates where (col_key='5005') or (col_key='-5000')");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			return 0;
		}


		public int	upd_mod_t_cod() 
		{
			try
			{
				backend.TransactionBegin();
				backend.dml("update updates "					+
							"set col_code = 'SQL+GROUPS' "		+
							"where col_key = 5005");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			return 0;
		}


		public int	upd_mod_t_end() 
		{
			try
			{
				backend.TransactionBegin();
				backend.dml( "update updates "	+
					"set col_key = -1000 where col_key = 1000000001");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			return 0;
		}


		public int	upd_mod_t_int() 
		{
			try
			{
				backend.TransactionBegin();
				backend.dml("update updates set col_int = 50015 where col_key = 5005");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			return 0; 
		}


		public int	upd_mod_t_mid() 
		{
			try
			{
				backend.TransactionBegin();
				backend.dml("update updates set col_key = '-5000' where col_key = 5005");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			return 0;
		}


		public int upd_remove_duplicate() 
		{
			try
			{
				backend.TransactionBegin();
				backend.dml("delete from updates where col_key = 6000 and col_int = 0");
				backend.TransactionCommit();
			}
			catch (Exception)
			{
				testFailed = true;
			}

			return 0;
		}

		#endregion
	}
}