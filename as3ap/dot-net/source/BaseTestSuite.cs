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
	#region Enumerations

	public enum IndexType
	{
		Btree,
		Clustered,
		Hash
	}

	#endregion

	#region Delegates

	public delegate void ResultEventHandler(object sender, TestResultEventArgs e);
	public delegate void ProgressEventHandler(object sender, ProgressMessageEventArgs e);

	#endregion

	public abstract class BaseTestSuite : ITestSuite
	{
		#region Events

		public event ResultEventHandler		Result;
		public event ProgressEventHandler	Progress;

		#endregion

		#region Fields

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

		#region Properties

		public Logger Log
		{
			get { return this.log; }
			set { this.log = value; }
		}

		public BenchMarkConfiguration Configuration
		{
			get { return this.configuration; }
			set { this.configuration = value; }
		}

		public int TupleCount
		{
			get { return this.tupleCount; }
			set { this.tupleCount = value;}
		}

		public string TestSuiteName
		{
			get { return this.testSuiteName; }
		}

		#endregion

		#region Constructors

		public BaseTestSuite(BenchMarkConfiguration configuration)
		{
			this.configuration	= configuration;

			// Set specific table structure
			this.baseTableStructure = baseTableStructure.Replace(
				"@INTEGER", configuration.IntegerTypeName);
			this.baseTableStructure = baseTableStructure.Replace(
				"@FLOAT", configuration.FloatTypeName);
			this.baseTableStructure = baseTableStructure.Replace(
				"@DOUBLE", configuration.DoubleTypeName);
			this.baseTableStructure = baseTableStructure.Replace(
				"@DECIMAL", configuration.DecimalTypeName);
			this.baseTableStructure = baseTableStructure.Replace(
				"@CHAR", configuration.CharTypeName);
			this.baseTableStructure = baseTableStructure.Replace(
				"@VARCHAR", configuration.VarcharTypeName);

			// Create the helper object
			this.dataHelper = DataHelperFactory.GetHelper(configuration.HelperType);
		}

		#endregion

		#region Finalizer

		~BaseTestSuite()
		{
			this.Dispose(false);
		}

		#endregion

		#region IDisposable Methods

		private void Dispose(bool disposing)
		{
			if (!this.disposed)			
			{
				if (disposing)
				{
					try
					{
						// release any managed resources					
						if (this.cursor != null)
						{
							this.cursor.Close();
							this.cursor = null;
						}
			
						if (this.cmdCursor != null)
						{
							this.cmdCursor.Dispose();
							this.cmdCursor = null;
						}
			
						if (this.transaction != null)
						{
							this.rollbackTransaction();
							this.transaction = null;
						}
						
						if (this.connection != null)
						{
							this.connection.Close();
							this.connection = null;
						}
						if (this.log != null)
						{
							if (this.userProcess != null)
							{
								foreach (Thread process in this.userProcess)
								{
									process.Abort();
									process.Join();
								}
							}							
						}

						this.configuration = null;
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
			this.Dispose(true);
			GC.SuppressFinalize(this);
		}

		#endregion

		#region Abstract Methods

		public abstract void join_2();
		
		public abstract void join_2_cl();

		public abstract void join_2_ncl();

		public abstract void join_3_cl();

		public abstract void join_3_ncl();

		public abstract void join_4_cl();

		public abstract void join_4_ncl();

		#endregion

		#region Misc Methods

		public int CountRows(string table)
		{
			StringBuilder	commandText = new StringBuilder();
			int				count = 0;

			commandText.AppendFormat("select count(col_key) from {0}", table);
			
			try
			{
				this.beginTransaction();
				this.cursorOpen(commandText.ToString());
				if (this.cursorFetch())
				{
					count = this.cursor.GetInt32(0);					
				}
			}
			catch(Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
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
				isolation = isolationLevel;
			}
		}

		#endregion

		#region Single User Tests

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_create_view() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("create view "								+
					"reportview(col_key,col_signed,col_date,col_decim, "	+
					"col_name,col_code,col_int) as "						+
					"select updates.col_key, updates.col_signed, "			+
					"updates.col_date, updates.col_decim, "					+
					"hundred.col_name, hundred.col_code, "					+
					"hundred.col_int "										+
					"from updates, hundred "								+
					"where updates.col_key = hundred.col_key");
				this.commitTransaction();
			}
			catch(Exception)
			{				
				this.testFailed = true;
			}

			this.testResult = 0; 
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_func() 
		{		
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select min(col_key) from hundred group by col_name");			
				while (this.cursorFetch()) 
				{
					count++;
				}
			}
			catch(Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_simple_report()
		{
			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select * "						+
					"from updates "					+
					"where updates.col_key in "		+
					"(select updates.col_key "		+
					"from updates, hundred "		+
					"where hundred.col_key = updates.col_key)");
				
				this.cursorFetch();

				this.testResult = cursor.GetValue(0);
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_info_retrieval() 
		{
			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select count(col_key) "									+
					"from tenpct "												+
					"where col_name = 'THE+ASAP+BENCHMARKS+' "					+
					"and col_int <= 100000000 "									+
					"and col_signed between 1 and 99999999 "					+
					"and not (col_float between -450000000 and 450000000) "		+
					"and col_double > 600000000 "								+
					"and col_decim < -600000000");

				this.cursorFetch();

				this.testResult = cursor.GetValue(0);
			}
			catch(Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_scal() 
		{	
			try
			{
				this.beginTransaction();
				this.cursorOpen("select min(col_key) from uniques");
				
				this.cursorFetch();
				
				this.testResult = cursor.GetValue(0);
			}
			catch (Exception)
			{
				this.testFailed = true;
				this.testResult = -1;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_subtotal_report() 
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select avg(col_signed), min(col_signed), max(col_signed), "	+
					"max(col_date), min(col_date), "								+
					"count(distinct col_name), count(col_name), "					+
					"col_code, col_int "											+
					"from reportview "												+
					"where col_decim >980000000 "									+
					"group by col_code, col_int");
				
				while (this.cursorFetch())
				{  
					count++;
				}
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_total_report() 
		{			
			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select avg(col_signed), min(col_signed), max(col_signed), "	+
					"max(col_date), min(col_date), "						+
					"count(distinct col_name), count(col_name), "			+
					"count(col_code), count(col_int) "						+
					"from reportview "												+
					"where col_decim >980000000");

				if (this.cursorFetch())
				{
					this.testResult = cursor.GetValue(0);
				}
				else
				{
					this.cursorClose();					
					this.rollbackTransaction();

					this.testFailed = true;
					this.testResult = -1;
				}
			}
			catch (Exception)
			{
				this.rollbackTransaction();
				this.testFailed = true;
				this.testResult = -1;
			}
			finally
			{
				this.cursorClose();
				if (!this.testFailed)
				{
					this.commitTransaction();
				}
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_append() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("insert into updates select * from saveupdates");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_delete() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("delete from updates where col_key < 0");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_modify() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("update updates "	+
					"set col_key = col_key - 100000 "		+
					"where col_key between 5000 and 5999");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_save()
		{
			try
			{
				this.createTable("saveupdates", baseTableStructure, null);

				this.beginTransaction();
				this.executeStatement("insert into saveupdates select * "	+
							"from updates where col_key between 5000 and 5999");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_hundred_code_h() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Btree		,
						"hundred_code_h"	, 
						"hundred"			, 
						"col_code");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_hundred_foreign() 
		{
			if (this.configuration.UseIndexes) 
			{  
				try
				{
					this.createForeignKey(
						"hundred"				, 
						"fk_hundred_updates"	, 
						"col_signed"			, 
						"updates"				, 
						"col_key");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_hundred_key_bt() 
		{
			if (this.configuration.UseIndexes && 
				this.configuration.SupportsClusteredIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Clustered	,
						"hundred_key_bt"	, 
						"hundred"			, 
						"col_key");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_code_h() 
		{
			if (this.configuration.UseIndexes && 
				this.configuration.SupportsHashIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Hash	,
						"tenpct_code_h"	, 
						"tenpct"		, 
						"col_code");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_decim_bt() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Btree		,
						"tenpct_decim_bt"	, 
						"tenpct"			, 
						"col_decim");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_double_bt() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Btree		,
						"tenpct_double_bt"	, 
						"tenpct"			, 
						"col_double");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0; 
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_float_bt() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Btree		,
						"tenpct_float_bt"	, 
						"tenpct"			, 
						"col_float");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_int_bt() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Btree	,
						"tenpct_int_bt"	, 
						"tenpct"		, 
						"col_int");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_key_bt() 
		{
			if (this.configuration.UseIndexes && 
				this.configuration.SupportsClusteredIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Clustered	,
						"tenpct_key_bt"		, 
						"tenpct"			, 
						"col_key");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_key_code_bt() 
		{
			if (this.configuration.UseIndexes)
			{
				try
				{
					this.createIndex(
						IndexType.Btree			,
						"tenpct_key_code_bt"	,
						"tenpct"				,
						"col_key, col_code");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_name_h() 
		{
			if (this.configuration.UseIndexes && 
				this.configuration.SupportsHashIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Hash	,
						"tenpct_name_h"	, 
						"tenpct"		,
						"col_name");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			} 
			
			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tenpct_signed_bt() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Btree		,
						"tenpct_signed_bt"	, 
						"tenpct"			,
						"col_signed");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			} 

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_tiny_key_bt() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Btree	,
						"tiny_key_bt"	, 
						"tiny"			, 
						"col_key");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_uniques_code_h() 
		{
			if (this.configuration.UseIndexes && 
				this.configuration.SupportsHashIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Hash		,
						"uniques_code_h"	, 
						"uniques"			, 
						"col_code");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		} 

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_uniques_key_bt() 
		{
			if (this.configuration.UseIndexes && 
				this.configuration.SupportsClusteredIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Clustered	,
						"uniques_key_bt"	, 
						"uniques"			, 
						"col_key");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		} 

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_updates_code_h() 
		{
			if (this.configuration.UseIndexes && 
				this.configuration.SupportsHashIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Hash		,
						"updates_code_h"	, 
						"updates"			, 
						"col_code");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_updates_decim_bt() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Btree		,
						"updates_decim_bt"	, 
						"updates"			,
						"col_decim");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_updates_double_bt() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Btree		,
						"updates_double_bt"	, 
						"updates"			,
						"col_double");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			} 
			this.testResult = 0;
		} 

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_updates_int_bt() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Btree	,
						"updates_int_bt", 
						"updates"		, 
						"col_int");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		} 

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_updates_key_bt() 
		{
			if (this.configuration.UseIndexes && 
				this.configuration.SupportsClusteredIndexes) 
			{
				try
				{
					this.createIndex(
						IndexType.Clustered	,
						"updates_key_bt"	, 
						"updates"			, 
						"col_key");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_tables() 
		{
			try
			{
				this.createTable("uniques", this.baseTableStructure, "col_key");
				this.createTable("hundred", this.baseTableStructure, "col_key");
				this.createTable("updates", this.baseTableStructure, "col_key");
				this.createTable("tenpct" , this.baseTableStructure, "col_key,col_code");

				this.createTable( 
					"tiny"					,
					"col_key " + this.configuration.IntegerTypeName + " not null",
					"col_key");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void drop_updates_keys() 
		{
			if (configuration.UseIndexes) 
			{
				try
				{
					this.beginTransaction();
					this.executeStatement("drop index updates_int_bt");
					this.executeStatement("drop index updates_double_bt");
					this.executeStatement("drop index updates_decim_bt");
					if (this.configuration.SupportsHashIndexes)
					{
						this.executeStatement("drop index updates_code_h");
					}
					this.commitTransaction();
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void integrity_test() 
		{
			try
			{
				this.createTable("integrity_temp", this.baseTableStructure, null);

				this.beginTransaction();
				this.executeStatement(
					"insert into integrity_temp select * from hundred where col_int = 0");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			try
			{
				this.beginTransaction();
				this.executeStatement("update hundred set col_signed = '-500000000' where col_int = 0");
				this.commitTransaction();

				this.testFailed = true;				
				this.testResult = 0;	
			}
			catch (Exception)
			{
			}

			try
			{
				this.beginTransaction();
				this.executeStatement("delete from hundred where col_int = 0");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			try
			{
				this.beginTransaction();
				this.executeStatement("insert into hundred select * from integrity_temp");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			try
			{
				this.beginTransaction();
				this.executeStatement("drop table integrity_temp");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void proj_100() 
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select distinct col_address, col_signed from hundred");
			
				while (this.cursorFetch()) 
				{
					count++;
				}				
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void proj_10pct() 
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen("select distinct col_signed from tenpct");
				
				while (this.cursorFetch()) 
				{
					count++;
				}				
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_1_cl() 
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_key = 1000");
				
				while (this.cursorFetch()) 
				{
					count++;
				}			
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count; 
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_1_ncl() 
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen( 
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_code = 'BENCHMARKS'");
				
				while (this.cursorFetch()) 
				{  
					count++;
				}
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_100_cl() 
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_key <= 100");
				
				while (this.cursorFetch()) 
				{
					count++;
				}
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_100_ncl() 
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_int <= 100");
				
				while (this.cursorFetch()) 
				{
					count++;
				}
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}


			this.testResult = count;
		}
	
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	sel_10pct_ncl() 
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen( 
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "						+
					"from tenpct "									+
					"where col_name = 'THE+ASAP+BENCHMARKS+'");
				
				while (this.cursorFetch()) 
				{  
					count++;
				}
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
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

				this.beginTransaction();
				this.cursorOpen(lineBuf.ToString());
				
				while (this.cursorFetch()) 
				{
					count++;
				}
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}


		public void sel_variable_select_high() 
		{
			this.sel_variable_select(-250000000);
		}


		public void sel_variable_select_low() 
		{
			this.sel_variable_select(-500000000);
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void table_scan() 
		{
			int count = 0;

			try
			{			
				this.beginTransaction();
				this.cursorOpen("select * from uniques where col_int = 1");
				
				while (this.cursorFetch()) 
				{  
					count++;
				}
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_app_t_end() 
		{
			int count = 0;
		
			try
			{
				this.beginTransaction();
				this.executeStatement("insert into updates "		+
					"values (1000000001, 50005, 50005, 50005.00, "	+
					"50005.00, 50005.00, '1/1/1988', "				+
					"'CONTROLLER', 'ALICE IN WONDERLAND', "			+
					"'UNIVERSITY OF ILLINOIS AT CHICAGO')"); 				
				this.commitTransaction();
				
				count++;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = count; 
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_app_t_mid() 
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.executeStatement("insert into updates "							+
					"values (5005, 5005, 50005, 50005.00, 50005.00, "	+
					"50005.00, '1/1/1988', 'CONTROLLER', "				+
					"'ALICE IN WONDERLAND', "							+
					"'UNIVERSITY OF ILLINOIS AT CHICAGO')");
				this.commitTransaction();

				count++;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = count;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_append_duplicate() 
		{
			if (this.configuration.UseIndexes) 
			{
				try
				{
					this.beginTransaction();
					this.executeStatement( "insert into updates  "		+
						"values (6000, 0, 60000, 39997.90, "				+
						"50005.00, 50005.00, "								+
						"'11/10/1985', 'CONTROLLER', "						+
						"'ALICE IN WONDERLAND', "							+
						"'UNIVERSITY OF ILLINOIS AT CHICAGO')"); 
					this.commitTransaction();

					this.testFailed = true;
				}
				catch (Exception)
				{
				}
			}

			this.testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_del_t_end() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("delete from updates where col_key = -1000"); 
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0; 
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_del_t_mid() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("delete from updates where (col_key='5005') or (col_key='-5000')");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_cod() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("update updates "	+
					"set col_code = 'SQL+GROUPS' "			+
					"where col_key = 5005");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_end() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement( "update updates "	+
					"set col_key = -1000 where col_key = 1000000001");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_int() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("update updates set col_int = 50015 where col_key = 5005");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0; 
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_mid() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("update updates set col_key = '-5000' where col_key = 5005");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_remove_duplicate() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("delete from updates where col_key = 6000 and col_int = 0");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		#endregion

		#region Multi User and Cross Section Tests

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void o_mode_tiny()
		{	
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen("select * from tiny");
			
				while (this.cursorFetch())
				{
					count++;
				}
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void o_mode_100k()
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen("select * from hundred where col_key<=1000");
			
				while (this.cursorFetch())
				{
					count++;
				}
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_checkmod_100_rand() 
		{
			object count = 0;
			
			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select count(*) from updates, sel100rand "		+
					"where updates.col_int=sel100rand.col_int "		+
					"and not updates.col_double=sel100rand.col_double");
			
				this.cursorFetch();

				count = this.cursor.GetValue(0);
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{					
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			if (Convert.ToInt32(count) != 100) 
			{
				this.testFailed = true;
			}

			this.testResult = count;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_drop_sel100_rand() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("drop table sel100rand");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_checkmod_100_seq() 
		{
			object count = 0;

			try
			{
				this.beginTransaction();
				this.cursorOpen(
					"select count(*) from updates, sel100seq "		+
					"where updates.col_key=sel100seq.col_key "		+
					"and not updates.col_double=sel100seq.col_double");
			
				this.cursorFetch();
			
				count = this.cursor.GetValue(0); 
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			if (Convert.ToInt32(count) != 100) 
			{
				this.testFailed = true;
			}

			this.testResult = count;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_drop_sel100_seq() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("drop table sel100seq");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
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

				this.beginTransaction();
				this.cursorOpen(lineBuf.ToString());
				if (!this.cursorFetch())
				{
					this.testFailed = true;
					count = 0;
				}
				else
				{
					count++;
				}
			}	
			catch (Exception)
			{
				this.testFailed = true;
			}
			finally
			{
				this.cursorClose();
				if (this.testFailed)
				{
					this.rollbackTransaction();
				}
				else
				{
					this.commitTransaction();
				}
			}

			this.testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_mod_100_rand() 
		{
			int count = 0;

			try
			{
				this.beginTransaction();
				this.executeStatement("update updates "	+
					"set col_double=col_double+100000000 "	+
					"where col_int between 1001 and 1100");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = count;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_mod_100_seq() 
		{
			try
			{
				this.beginTransaction();					
				this.executeStatement("update updates "		+
					"set col_double = col_double+100000000 "	+
					"where col_key between 1001 and 1100");
				this.rollbackTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
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

				this.beginTransaction();
				this.executeStatement(lineBuf.ToString());
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_sel_100_rand() 
		{
			try
			{
				createTable("sel100rand", baseTableStructure, null);

				this.beginTransaction();
				this.executeStatement("insert into sel100rand select * from updates "	+
					"where updates.col_int between 1001 and 1100");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_sel_100_seq() 
		{
			try
			{
				createTable("sel100seq", baseTableStructure, null);

				this.beginTransaction();
				this.executeStatement("insert into sel100seq select * from updates "	+
					"where updates.col_key between 1001 and 1100");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_unmod_100_rand() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("update updates "	+
					"set col_double=col_double-100000000 "	+
					"where col_int between 1001 and 1100");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_unmod_100_seq() 
		{
			try
			{
				this.beginTransaction();
				this.executeStatement("update updates "	+
					"set col_double=col_double-100000000 "	+
					"where col_key between 1001 and 1100");
				this.commitTransaction();
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0; 
		}

		#endregion

		#region AS3AP Methods

		public void CreateDatabase() 
		{
			this.dropDatabase();
			this.databaseCreate();

			this.DatabaseConnect();

			this.runTest("create_tables");
			this.runTest("load_data");
			this.runTest("create_idx_uniques_key_bt");
			this.runTest("create_idx_updates_key_bt");
			this.runTest("create_idx_hundred_key_bt");
			this.runTest("create_idx_tenpct_key_bt");
			this.runTest("create_idx_tenpct_key_code_bt");
			this.runTest("create_idx_tiny_key_bt");
			this.runTest("create_idx_tenpct_int_bt");
			this.runTest("create_idx_tenpct_signed_bt");
			this.runTest("create_idx_uniques_code_h");
			this.runTest("create_idx_tenpct_double_bt");
			this.runTest("create_idx_updates_decim_bt");
			this.runTest("create_idx_tenpct_float_bt");
			this.runTest("create_idx_updates_int_bt");
			this.runTest("create_idx_tenpct_decim_bt");
			this.runTest("create_idx_hundred_code_h");
			this.runTest("create_idx_tenpct_name_h");
			this.runTest("create_idx_updates_code_h");
			this.runTest("create_idx_tenpct_code_h");
			this.runTest("create_idx_updates_double_bt");
			this.runTest("create_idx_hundred_foreign");
			
			this.DatabaseDisconnect();
		}

		public void SingleUserTests() 
		{
			long		clocks;
			TimeSpan	elapsed;

			this.DatabaseConnect();
			
			clocks = System.DateTime.Now.Ticks;

			this.runTest("sel_1_cl");
			this.runTest("join_3_cl");
			this.runTest("sel_100_ncl");
			this.runTest("table_scan");
			this.runTest("agg_func");
			this.runTest("agg_scal");
			this.runTest("sel_100_cl");
			this.runTest("join_3_ncl");
			this.runTest("sel_10pct_ncl");
			this.runTest("agg_simple_report");
			this.runTest("agg_info_retrieval");
			this.runTest("agg_create_view");
			this.runTest("agg_subtotal_report");
			this.runTest("agg_total_report");
			this.runTest("join_2_cl");
			this.runTest("join_2");
			this.runTest("sel_variable_select_low");
			this.runTest("sel_variable_select_high");
			this.runTest("join_4_cl");
			this.runTest("proj_100");
			this.runTest("join_4_ncl");
			this.runTest("proj_10pct");
			this.runTest("sel_1_ncl");
			this.runTest("join_2_ncl");
			this.runTest("integrity_test");
			this.runTest("drop_updates_keys");
			this.runTest("bulk_save");
			this.runTest("bulk_modify");
			this.runTest("upd_append_duplicate");
			this.runTest("upd_remove_duplicate");
			this.runTest("upd_app_t_mid");
			this.runTest("upd_mod_t_mid");
			this.runTest("upd_del_t_mid");
			this.runTest("upd_app_t_end");
			this.runTest("upd_mod_t_end");
			this.runTest("upd_del_t_end");
			this.runTest("create_idx_updates_code_h");
			this.runTest("upd_app_t_mid");
			this.runTest("upd_mod_t_cod");
			this.runTest("upd_del_t_mid");
			this.runTest("create_idx_updates_int_bt");
			this.runTest("upd_app_t_mid");
			this.runTest("upd_mod_t_int");
			this.runTest("upd_del_t_mid");
			this.runTest("bulk_append");
			this.runTest("bulk_delete");

			elapsed = new TimeSpan(DateTime.Now.Ticks - clocks);

			if (log != null) log.Simple("\r\nSingle user test ( {0} )\r\n\r\n",
								 elapsed.ToString());

			this.DatabaseDisconnect();
		}

		public void MultiUserTests(int nInstances) 
		{	
			TimeSpan	fTime;
			long		sTime;			

			this.userProcess = new Thread[nInstances];
			
			if (this.log != null)
			{
				this.log.Simple(
					"\"Executing multi-user tests with {0} user task{1}\"\r\n\r\n",
					nInstances, ((nInstances != 1) ? "s" : ""));
			}

			/* Step 1 -- Backup updates relation, including indices, 
			 * to tape or other device. This is done early on.
			 */
			
						
			/* Step 2 -- Run IR (Mix 1) test for 15 minutes.	*/
			if (this.Progress != null)
			{
				this.Progress(
					this, 
					new ProgressMessageEventArgs("Run IR (Mix 1) test for 15 minutes (" + DateTime.Now.ToString() + ")."));
			}

			this.iters		= 0;
			this.timeToRun	= 15;
					
			for (int i = 0; i < this.userProcess.Length; i++) 
			{
				this.userProcess[i]	= new Thread(new ThreadStart(this.ir_select));

				this.userProcess[i].Name = "User " + i.ToString();
				this.userProcess[i].Start();
				this.userProcess[i].IsBackground = true;
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < this.userProcess.Length; i++) 
			{
				this.userProcess[i].Join();
			}

			/* Step 3 -- Measure throughput in IR test for five minutes.	*/
			if (this.Progress != null)
			{
				this.Progress(
					this, 
					new ProgressMessageEventArgs("Run Measure throughput in IR test for five minutes (" + DateTime.Now.ToString() + ")."));
			}
			
			sTime			= DateTime.Now.Ticks;
			this.iters		= 0;			
			this.timeToRun	= 5;
						
			for (int i = 0; i < this.userProcess.Length; i++)
			{
				this.userProcess[i] = new Thread(new ThreadStart(this.ir_select));

				this.userProcess[i].Name = "User " + i.ToString();
				this.userProcess[i].Start();
				this.userProcess[i].IsBackground = true;
			}
						
			/* Wait to the end of the threads	*/
			for (int i = 0; i < this.userProcess.Length; i++) 
			{
				this.userProcess[i].Join();
			}

			fTime = new TimeSpan(DateTime.Now.Ticks - sTime);
			if (this.log != null)
			{
				this.log.Simple(
					"Mixed IR (tup/sec)\t{0}"			+
					"\treturned in {1} minutes"			,
					Math.Round((double)iters/fTime.TotalSeconds, 4)	, 
					fTime.ToString());
			}
			
			/* Step 4 -- A Mixed Workload IR Test, where one user executes a cross
			 * section of ten update and retrieval queries, and all the others 
			 * execute the same IR query as in the second test.
			 */
			if (this.Progress != null)
			{
				this.Progress(
					this, 
					new ProgressMessageEventArgs("Run Mixed Workload IR test (Mix 3) (" + DateTime.Now.ToString() + ")."));
			}
			
			this.userProcess[0]		= new Thread(new ThreadStart(this.cross_section_tests));

			this.userProcess[0].Name = "User " + 0.ToString();
			this.userProcess[0].Start();
			this.userProcess[0].IsBackground = true;
			
			this.timeToRun	= -1;	// Exec the only one time in each thread
			for (int i = 1; i < this.userProcess.Length; i++) 
			{
				this.userProcess[i] = new Thread(new ThreadStart(this.ir_select));

				this.userProcess[i].Name = "User " + i.ToString();
				this.userProcess[i].Start();
				this.userProcess[i].IsBackground = true;
			}
						
			/* Wait to the end of the threads	*/
			for (int i = 0; i < this.userProcess.Length; i++) 
			{
				this.userProcess[i].Join();
			}
						
			/* Step 5 -- Run queries to check correctness of the sequential
			 * and random bulk updates.
			 */
			if (this.Progress != null)
			{
				Progress(
					this, 
					new ProgressMessageEventArgs("Check correctness of the sequential and random bulk updates (" + DateTime.Now.ToString() + ")."));
			}
			
			this.DatabaseConnect();

			this.runTest("mu_checkmod_100_seq");
			this.runTest("mu_checkmod_100_rand");

			this.DatabaseDisconnect();
			
			/* Step 6 - Recover updates relation from backup tape (Step 1)
			 * and log (from Steps 2, 3, 4, and 5).	
			 */


			/* Step 7 - Perform correctness checks, checkmod_100_seq and 
			 * checkmod_100_rand. Remove temporary tables: sel100seq and 
			 * sel100rand.
			 */
			if (this.Progress != null)
			{
				this.Progress(
					this, 
					new ProgressMessageEventArgs("Check correctness of the sequential and random bulk updates (" + DateTime.Now.ToString() + ")."));
			}
			
			this.DatabaseConnect();

			this.runTest("mu_checkmod_100_seq");
			this.runTest("mu_checkmod_100_rand");

			this.runTest("mu_drop_sel100_seq");
			this.runTest("mu_drop_sel100_rand");

			this.DatabaseDisconnect();

			/* Step 8 - Run OLTP test for 15 minutes.	*/
			if (this.Progress != null)
			{
				this.Progress(
					this, 
					new ProgressMessageEventArgs("Run OLTP test for 15 minutes (" + DateTime.Now.ToString() + ")."));
			}
			
			this.timeToRun = 15;						
			for (int i = 0; i < this.userProcess.Length; i++) 
			{
				this.userProcess[i] = new Thread(new ThreadStart(oltp_update));

				this.userProcess[i].Name = "User " + i.ToString();
				this.userProcess[i].Start();
				this.userProcess[i].IsBackground = true;
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < this.userProcess.Length; i++) 
			{
				this.userProcess[i].Join();
			}
			
			/* Step 9 -- Measure throughput in IR test for five minutes.	*/
			if (this.Progress != null)
			{
				this.Progress(
					this, 
					new ProgressMessageEventArgs("Run Measure throughput in IR test for five minutes (" + DateTime.Now.ToString() + ")."));
			}

			sTime			= DateTime.Now.Ticks;
			this.iters		= 0;
			this.timeToRun	= 5;
			
			for (int i = 0; i < this.userProcess.Length; i++)
			{
				this.userProcess[i] = new Thread(new ThreadStart(this.ir_select));

				this.userProcess[i].Name = "User " + i.ToString();
				this.userProcess[i].Start();
				this.userProcess[i].IsBackground = true;
			}
			
			/* Wait to the end of the threads	*/
			for (int i = 0; i < this.userProcess.Length; i++) 
			{
				this.userProcess[i].Join();
			}
						
			fTime = new TimeSpan(DateTime.Now.Ticks - sTime);
			
			if (this.log != null)
			{
				this.log.Simple(
					"Mixed OLTP (tup/sec)\t{0}"			+
					"\treturned in {1} minutes\n"		,
					Math.Round((double)iters/fTime.TotalSeconds, 4)	, 
					fTime.ToString());
			}

			/* Step 10 -- Replace one background OLTP script with the cross 
			 * section script. This is the Mixed Workload OLTP test (Mix 4). 
			 * This step is variable length.
			 */
			if (this.Progress != null)
			{
				this.Progress(
					this, 
					new ProgressMessageEventArgs("Run Mixed Workload OLTP test (Mix 4) (" + DateTime.Now.ToString() + ")."));
			}
						
			this.userProcess[0] = new Thread(new ThreadStart(this.cross_section_tests));
			
			this.userProcess[0].Name = "User " + 0.ToString();
			this.userProcess[0].Start();
			this.userProcess[0].IsBackground = true;

			this.timeToRun = -1;	// Exec the only one time in each thread

			for (int i = 1; i < this.userProcess.Length; i++)
			{
				this.userProcess[i] = new Thread(new ThreadStart(this.oltp_update));

				this.userProcess[i].Name = "User " + i.ToString();
				this.userProcess[i].Start();
				this.userProcess[i].IsBackground = true;
			}			

			/* Wait to the end of the threads	*/
			for (int i = 0; i < this.userProcess.Length; i++) 
			{
				this.userProcess[i].Join();
			}
									
			/* Step 11 -- Perform correctness checks, checkmod_100_seq and 
			 * checkmod_100_rand. Remove temporary tables: sel100seq and 
			 * sel100rand.
			 */
			if (this.Progress != null)
			{
				this.Progress(
					this, 
					new ProgressMessageEventArgs("Check correctness of the sequential and random bulk updates (" + DateTime.Now.ToString() + ")."));
			}
			
			this.DatabaseConnect();

			this.runTest("mu_checkmod_100_seq");			
			this.runTest("mu_checkmod_100_rand");
			
			this.runTest("mu_drop_sel100_seq");
			this.runTest("mu_drop_sel100_rand");

			this.DatabaseDisconnect();
		}

		private void cross_section_tests() 
		{
			try
			{
				long	startTime;
				long	endTime;

				this.DatabaseConnect();

				startTime = DateTime.Now.Ticks;

				this.runTest("o_mode_tiny");
				this.runTest("o_mode_100k");
				this.runTest("sel_1_ncl");
				this.runTest("sel_1_ncl");
				this.runTest("sel_1_ncl");
				this.runTest("agg_simple_report");
				this.runTest("mu_sel_100_seq");
				this.runTest("mu_sel_100_rand");
				this.runTest("mu_mod_100_seq");
				this.runTest("mu_mod_100_rand");
				this.runTest("mu_unmod_100_seq");
				this.runTest("mu_unmod_100_rand");

				endTime = DateTime.Now.Ticks;
				TimeSpan elapsed = new TimeSpan(endTime - startTime);

				this.DatabaseDisconnect();

				if (this.log != null)
				{
					this.log.Simple("CrossSectionTests \t( {1} )", elapsed.ToString());
				}
			}
			catch (ThreadAbortException)
			{
			}
		}

		private void ir_select()
		{	
			try
			{
				ITestSuite testSuite = TestSuiteFactory.GetTestSuite(testSuiteName, configuration);

				testSuite.DatabaseConnect();
			
				DateTime endTime = DateTime.Now;

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
					this.mu_ir_select();
				}
			
				testSuite.DatabaseDisconnect();
			}
			catch (ThreadAbortException)
			{
			}
		}
		
		private void oltp_update()
		{	
			try
			{
				ITestSuite testSuite = TestSuiteFactory.GetTestSuite(
					testSuiteName, configuration);
			
				testSuite.DatabaseConnect();
			
				DateTime endTime = DateTime.Now;

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
			catch (ThreadAbortException)
			{
			}
		}

		private void runTest(string testName)
		{
			Type 		type	= null;
			MethodInfo	method	= null;
			long		clocks	= 0;

			this.testFailed	= false;

			type	= this.GetType();
			method 	= type.GetMethod(
				testName, 
				BindingFlags.Public|BindingFlags.NonPublic|BindingFlags.Instance);
			
			// Set IsolationLevel for test execution
			this.SetIsolationLevel(testName);

			// Reset this.testFailed property value
			this.testFailed = false;

			clocks = DateTime.Now.Ticks;

			method.Invoke(this, null);

			clocks	= DateTime.Now.Ticks - clocks;

			testName = formatTestName(testName);

			TimeSpan elapsed = new TimeSpan(clocks);				

			if (this.Result != null)
			{
				this.Result(
					this, 
					new TestResultEventArgs(
						testName, this.testResult, elapsed, this.testFailed));
			}

			StringBuilder logMessage = new StringBuilder();

			if (this.testFailed)
			{
				if (log != null) log.Simple("-----> {0}\tfailed <-----", testName);
			}
			else
			{
				logMessage.AppendFormat(
					"{0} ( {1} )\treturn value = {2} \t\t"	,
					testName								,
					elapsed.ToString(),
					this.testResult);
			}

			if (log != null)
			{
				log.Simple(logMessage.ToString());
			}

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
		
		#region Database Creation Methods

		private void databaseCreate()
		{
			this.dataHelper.CreateDatabase(this.parseConnectionString());
		}

		private void dropDatabase()
		{
			try
			{
				this.dataHelper.DropDatabase(this.parseConnectionString());
			}
			catch (Exception)
			{
			}
		}

		private Hashtable parseConnectionString()
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
			string	isecurity	= "SSPI";

			Regex			search	 = new Regex(@"([\w\s\d]*)\s*=\s*([^;]*)");
			MatchCollection	elements = search.Matches(this.configuration.ConnectionString);

			foreach (Match element in elements)
			{
				switch (element.Groups[1].Value.Trim().ToLower())
				{
					case "datasource":
					case "data source":
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

					case "Integrated Security":
						isecurity = element.Groups[2].Value.Trim();
						break;
				}
			}

			Hashtable values = new Hashtable();

			values.Add("DataSource"			, dataSource);
			values.Add("Port"				, port);
			values.Add("Database"			, database);
			values.Add("User"				, user);
			values.Add("Password"			, password);
			values.Add("Dialect"			, dialect);
			values.Add("ForcedWrites"		, forcedWrite);
			values.Add("PageSize"			, pageSize);
			values.Add("Charset"			, charset);
			values.Add("ServerType"			, serverType);
			values.Add("SSL"				, ssl);
			values.Add("Integrated Security", isecurity);

			return values;
		}

		private void createIndex(IndexType indextype, string indexName, string tableName, string fields)
		{
			string createIndexStmt = String.Empty;
			
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
				this.beginTransaction();
				this.executeStatement(createIndexStmt);
				this.commitTransaction();
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
				this.beginTransaction();
				this.executeStatement(commandText.ToString());
				this.commitTransaction();
			}
			catch(Exception ex)
			{
				if (log != null)
				{
					log.Error("foreign key error {0}", ex.Message);
				}
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

				this.beginTransaction();
				this.executeStatement(commandText.ToString());
				this.commitTransaction();
			}
			catch (Exception ex)
			{
				if (log != null)
				{
					log.Error("error create table {0}", ex.Message);
				}
				throw ex;
			}
		}

		protected void cursorOpen(string commandText)
		{
			try
			{
				this.cmdCursor	= this.getCommand(commandText);
				this.cursor		= this.cmdCursor.ExecuteReader();
			}
			catch (Exception ex)
			{
				if (this.log != null) 
				{
					log.Error("this.cursorOpen failed {0}", ex.Message);
				}

				if (this.cursor != null)
				{
					this.cursor.Close();
					this.cursor = null;
				}

				if (this.cmdCursor != null)
				{
					this.cmdCursor.Dispose();
					this.cmdCursor = null;
				}
				
				throw ex;
			}
		}

		protected bool cursorFetch()
		{
			bool fetched = false;

			try
			{
				fetched = this.cursor.Read();
			}
			catch (Exception ex)
			{
				if (log != null)
				{
					log.Error("this.cursorFetch failed {0}", ex.Message);
				}
			}

			return fetched;
		}
		
		protected void cursorClose()
		{
			try
			{
				if (this.cursor != null)
				{
					this.cursor.Close();
				}
			}
			catch(Exception ex)
			{				
				if (log != null) 
				{
					log.Error("cursorClose failed {0}", ex.Message);
				}

				throw ex;
			}
			finally
			{
				if (this.cursor != null)
				{
					this.cursor.Close();
					this.cursor = null;
				}

				if (this.cmdCursor != null)
				{
					this.cmdCursor.Dispose();
					this.cmdCursor = null;
				}
			}
		}

		public void DatabaseConnect()
		{
			try
			{
				this.connection = this.dataHelper.CreateConnection(
					this.configuration.ConnectionString);
				this.connection.Open();
			}
			catch (Exception ex)
			{
				if (this.log != null) 
				{
					this.log.Error("DatabaseConnect error {0}", ex.Message);
				}
				throw ex;
			}
		}

		public void DatabaseDisconnect()
		{
			try
			{
				if (this.transaction != null)
				{
					this.transaction.Rollback();
					this.transaction = null;
				}

				if (this.connection != null)
				{
					this.connection.Close();
					this.connection	= null;
				}
			}
			catch (Exception ex)
			{
				if (log != null)
				{
					log.Error("disconnect error {0}", ex.Message);
				}
				throw ex;
			}
		}

		protected void executeStatement(string commandText)
		{			
			IDbCommand command = null;

			try
			{
				command = this.getCommand(commandText);
				command.ExecuteNonQuery();
			}
			catch (Exception ex)
			{
				this.rollbackTransaction();
				
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
				this.transaction = connection.BeginTransaction(isolation);
			}
			catch(Exception ex)
			{
				if (log != null)
				{
					log.Error("this.beginTransaction failed {0}", ex.Message);
				}
				throw ex;
			}
		}

		protected void commitTransaction()
		{
			try
			{
				this.transaction.Commit();
				this.transaction = null;
			}
			catch (Exception ex)
			{					
				if (this.log != null)
				{
					log.Error("Commit failed {0}", ex.Message);
				}
				throw ex;
			}
		}

		protected void rollbackTransaction()
		{
			try
			{
				this.transaction.Rollback();
				this.transaction = null;
			}
			catch (Exception ex)
			{
				if (log != null)
				{
					log.Error("Rollback failed {0}", ex.Message);
				}
				throw ex;
			}
		}

		public void load_data()
		{
			try
			{
				DateTime start = DateTime.Now;

				this.loadTinyFile("tiny");

				if (this.Result != null)
				{
					this.Result(
						this, 
						new TestResultEventArgs(
							"	tiny file loaded", 
							0, 
							DateTime.Now - start, false));
				}

				start = DateTime.Now;
								
				this.loadFile("uniques");

				if (this.Result != null)
				{
					this.Result(this, 
						new TestResultEventArgs(
						"	uniques file loaded", 
						0, 
						DateTime.Now - start, false));
				}

				start = DateTime.Now;
				
				this.loadFile("updates");

				if (this.Result != null)
				{
					this.Result(
						this, 
						new TestResultEventArgs(
						"	updates file loaded", 
						0, 
						DateTime.Now - start, false));
				}

				start = DateTime.Now;
				
				this.loadFile("hundred");

				if (this.Result != null)
				{
					this.Result(
						this, 
						new TestResultEventArgs(
						"	hundred file loaded", 
						0, 
						DateTime.Now - start, false));
				}

				start = DateTime.Now;
				
				this.loadFile("tenpct");

				if (this.Result != null)
				{
					this.Result(
						this, 
						new TestResultEventArgs(
						"	tenpct file loaded", 
						0, 
						DateTime.Now - start, false));
				}
			}
			catch (Exception ex)
			{
				if (log != null) 
				{
					log.Error("load failed {0}", ex.Message);
				}

				this.rollbackTransaction();

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
			command = this.getCommand(commandText.ToString());
			
			/* Add parameters	*/
			command.Parameters.Add(dataHelper.CreateParameter("@col_key", DbType.Int32, 4, "col_key"));
			command.Parameters.Add(dataHelper.CreateParameter("@col_int", DbType.Int32, 4, "col_int"));
			command.Parameters.Add(dataHelper.CreateParameter("@col_signed", DbType.Int32, 4, "col_signed"));
			command.Parameters.Add(dataHelper.CreateParameter("@col_float", DbType.Single, 4, "col_float"));
			command.Parameters.Add(dataHelper.CreateParameter("@col_double", DbType.Double, 8, "col_double"));
			command.Parameters.Add(dataHelper.CreateParameter("@col_decim", DbType.Single, 18, 2, "col_decim"));
			command.Parameters.Add(dataHelper.CreateParameter("@col_date", DbType.StringFixedLength, 20, "col_date"));
			command.Parameters.Add(dataHelper.CreateParameter("@col_code", DbType.StringFixedLength, 10, "col_code"));
			command.Parameters.Add(dataHelper.CreateParameter("@col_name", DbType.StringFixedLength, 20, "col_name"));
			command.Parameters.Add(dataHelper.CreateParameter("@col_address", DbType.String, 80, "col_address"));

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
					this.beginTransaction();
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
					this.commitTransaction();
					transactionPending	= false;
					rowCount			= 0;
				}
			}

			if (transactionPending)
			{
				this.commitTransaction();
			}

			command.Dispose();
			stream.Close();
		}

		private void loadTinyFile(string table)
		{
			this.beginTransaction();

			StringBuilder	commandText = new StringBuilder();
			StreamReader	stream		= null;
			IDbCommand		command		= null;

			commandText.AppendFormat("insert into {0} values (@col_key)", table);

			/* Crate command */
			command = this.getCommand(commandText.ToString());

			/* Add parameters	*/
			IDbDataParameter p = dataHelper.CreateParameter("@col_key", DbType.Int32, 4, "col_key");
			command.Parameters.Add(p);

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

			this.commitTransaction();
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
