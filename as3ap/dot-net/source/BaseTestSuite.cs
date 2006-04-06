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
using System.Collections;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Text;
using System.Reflection;
using System.Text.RegularExpressions;

namespace DatabaseBenchmark
{
	public abstract class BaseTestSuite : ITestSuite
	{
		#region · Events ·

		public event ResultEventHandler		Result;
		public event ProgressEventHandler	Progress;

		#endregion

		#region · Fields ·

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

		private	Logger		    log;
		private int			    iters		= 0;
		private int			    timeToRun	= 15;
		private bool		    disposed	= false;
		private int			    tupleCount	= 0;		
		private	IsolationLevel	isolation   = IsolationLevel.ReadCommitted;
		private	DbConnection	connection  = null;

		#endregion

        #region · Protected Fields ·

        protected bool              testFailed      = false;
        protected object            testResult      = 0;
        protected string            testSuiteName   = String.Empty;
        protected DbProviderFactory providerFactory = null;

        #endregion

        #region · Properties ·

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

		#region · Constructors ·

		public BaseTestSuite(BenchMarkConfiguration configuration)
		{
			this.configuration	= configuration;

			// Set specific table structure
			this.baseTableStructure = baseTableStructure.Replace("@INTEGER", configuration.IntegerTypeName);
			this.baseTableStructure = baseTableStructure.Replace("@FLOAT", configuration.FloatTypeName);
			this.baseTableStructure = baseTableStructure.Replace("@DOUBLE", configuration.DoubleTypeName);
			this.baseTableStructure = baseTableStructure.Replace("@DECIMAL", configuration.DecimalTypeName);
			this.baseTableStructure = baseTableStructure.Replace("@CHAR", configuration.CharTypeName);
			this.baseTableStructure = baseTableStructure.Replace("@VARCHAR", configuration.VarcharTypeName);

            // Get an instance of the provider factory
            string providerName = ConfigurationManager.ConnectionStrings[this.Configuration.ConnectionStringName].ProviderName;
            
            this.providerFactory = DbProviderFactories.GetFactory(providerName);
		}

		#endregion

		#region · Finalizer ·

		~BaseTestSuite()
		{
			this.Dispose(false);
		}

		#endregion

		#region · IDisposable Methods ·

		private void Dispose(bool disposing)
		{
			if (!this.disposed)			
			{
				if (disposing)
				{
					try
					{
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

		#region · Misc Methods ·

		public int CountRows(string table)
		{
			int		count	= 0;
			string	sql		= "select count(col_key) from " + table;
			
			try
			{
				count = Convert.ToInt32(this.ExecuteScalar(sql));
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			return count;
		}

		public void SetIsolationLevel(string methodName)
		{
			IsolationLevel			isolationLevel = IsolationLevel.ReadCommitted;
			IsolationLevelAttribute att;

			try
			{
				att = (IsolationLevelAttribute)Attribute.GetCustomAttribute(GetType().GetMethod(methodName), typeof(IsolationLevelAttribute));
                if (att != null)
                {
                    isolationLevel = att.IsolationLevel;
                }
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

		#region · AS3AP Methods ·

		public void CreateDatabase() 
		{
            string connectionString = ConfigurationManager.ConnectionStrings[this.configuration.ConnectionStringName].ConnectionString;
            string providerName = ConfigurationManager.ConnectionStrings[this.configuration.ConnectionStringName].ProviderName;
            TimeSpan elapsed = new TimeSpan(0, 0, 0);

			this.DropDatabase();

            if (providerName.ToLower().Trim() == "firebirdsql.data.firebirdclient")
            {
                FirebirdSql.Data.FirebirdClient.FbConnection.CreateDatabase(connectionString, true);
            }
            else
            {
                string                      catalog = "";
                DbConnectionStringBuilder   builder = this.providerFactory.CreateConnectionStringBuilder();

                builder.ConnectionString = connectionString;

                if (builder.ContainsKey("Catalog"))
                {
                    catalog = builder["Catalog"].ToString();
                    builder["Catalog"] = "";
                }
                else if (builder.ContainsKey("Database"))
                {
                    catalog = builder["Database"].ToString();
                    builder["Database"] = "";
                }

                DbConnection connection = this.providerFactory.CreateConnection();
                connection.ConnectionString = builder.ToString();
                connection.Open();

                DbCommand command = connection.CreateCommand();

                try
                {
                    command.CommandText = String.Format(this.Configuration.CreateDatabaseStmt, catalog);
                    command.ExecuteNonQuery();
                }
                catch
                {
                    throw;
                }
                finally
                {
                    connection.Close();
                }
            }
			
			this.ConnectDatabase();

			elapsed += this.runTest("create_tables");
			elapsed += this.runTest("load_data");
			elapsed += this.runTest("create_idx_uniques_key_bt");
			elapsed += this.runTest("create_idx_updates_key_bt");
			elapsed += this.runTest("create_idx_hundred_key_bt");
			elapsed += this.runTest("create_idx_tenpct_key_bt");
			elapsed += this.runTest("create_idx_tenpct_key_code_bt");
			elapsed += this.runTest("create_idx_tiny_key_bt");
			elapsed += this.runTest("create_idx_tenpct_int_bt");
			elapsed += this.runTest("create_idx_tenpct_signed_bt");
			elapsed += this.runTest("create_idx_uniques_code_h");
			elapsed += this.runTest("create_idx_tenpct_double_bt");
			elapsed += this.runTest("create_idx_updates_decim_bt");
			elapsed += this.runTest("create_idx_tenpct_float_bt");
			elapsed += this.runTest("create_idx_updates_int_bt");
			elapsed += this.runTest("create_idx_tenpct_decim_bt");
			elapsed += this.runTest("create_idx_hundred_code_h");
			elapsed += this.runTest("create_idx_tenpct_name_h");
			elapsed += this.runTest("create_idx_updates_code_h");
			elapsed += this.runTest("create_idx_tenpct_code_h");
			elapsed += this.runTest("create_idx_updates_double_bt");
			elapsed += this.runTest("create_idx_hundred_foreign");
			
			this.DisconnectDatabase();

			if (this.log != null) 
			{
				this.log.Simple("\r\nDatabase creation time ( {0} )\r\n\r\n", elapsed.ToString());
			}
		}

		private TimeSpan runTest(string testName)
		{
			Type 		type	= null;
			MethodInfo	method	= null;
			DateTime	stime	= DateTime.Now;

			this.testFailed	= false;

			type	= this.GetType();
			method 	= type.GetMethod(testName, BindingFlags.Public|BindingFlags.NonPublic|BindingFlags.Instance);
			
			// Set IsolationLevel for test execution
			this.SetIsolationLevel(testName);

			// Reset this.testFailed property value
			this.testFailed = false;

			stime = DateTime.Now;

			method.Invoke(this, null);

			TimeSpan elapsed = DateTime.Now - stime;

			testName = formatTestName(testName);

			if (this.Result != null)
			{
				this.Result(this, new TestResultEventArgs(testName, this.testResult, elapsed, this.testFailed));
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

			if (this.log != null)
			{
				this.log.Simple(logMessage.ToString());
			}

			return elapsed;
		}

		private string formatTestName(string methodName)
		{
			int length = 30 - methodName.Length;

			for (int i = 0; i < length; i++)
			{
				methodName = String.Format(" {0}", methodName);
			}

			return methodName;
		}

		#endregion

		#region · Single User Tests - Main ·

		public void SingleUserTests() 
		{
			TimeSpan elapsed = new TimeSpan(0, 0, 0);

			this.ConnectDatabase();
						
			elapsed += this.runTest("sel_1_cl");
			elapsed += this.runTest("join_3_cl");
			elapsed += this.runTest("sel_100_ncl");
			elapsed += this.runTest("table_scan");
			elapsed += this.runTest("agg_func");
			elapsed += this.runTest("agg_scal");
			elapsed += this.runTest("sel_100_cl");
			elapsed += this.runTest("join_3_ncl");
			elapsed += this.runTest("sel_10pct_ncl");
			elapsed += this.runTest("agg_simple_report");
			elapsed += this.runTest("agg_info_retrieval");
			elapsed += this.runTest("agg_create_view");
			elapsed += this.runTest("agg_subtotal_report");
			elapsed += this.runTest("agg_total_report");
			elapsed += this.runTest("join_2_cl");
			elapsed += this.runTest("join_2");
			elapsed += this.runTest("sel_variable_select_low");
			elapsed += this.runTest("sel_variable_select_high");
			elapsed += this.runTest("join_4_cl");
			elapsed += this.runTest("proj_100");
			elapsed += this.runTest("join_4_ncl");
			elapsed += this.runTest("proj_10pct");
			elapsed += this.runTest("sel_1_ncl");
			elapsed += this.runTest("join_2_ncl");
			elapsed += this.runTest("integrity_temp");
			elapsed += this.runTest("integrity_test");
			elapsed += this.runTest("integrity_restore");
			elapsed += this.runTest("drop_updates_keys");
			elapsed += this.runTest("bulk_save");
			elapsed += this.runTest("bulk_modify");
			elapsed += this.runTest("upd_append_duplicate");
			elapsed += this.runTest("upd_remove_duplicate");
			elapsed += this.runTest("upd_app_t_mid");
			elapsed += this.runTest("upd_mod_t_mid");
			elapsed += this.runTest("upd_del_t_mid");
			elapsed += this.runTest("upd_app_t_end");
			elapsed += this.runTest("upd_mod_t_end");
			elapsed += this.runTest("upd_del_t_end");
			elapsed += this.runTest("create_idx_updates_code_h");
			elapsed += this.runTest("upd_app_t_mid");
			elapsed += this.runTest("upd_mod_t_cod");
			elapsed += this.runTest("upd_del_t_mid");
			elapsed += this.runTest("create_idx_updates_int_bt");
			elapsed += this.runTest("upd_app_t_mid");
			elapsed += this.runTest("upd_mod_t_int");
			elapsed += this.runTest("upd_del_t_mid");
			elapsed += this.runTest("bulk_append");
			elapsed += this.runTest("bulk_delete");

			if (this.log != null) 
			{
				this.log.Simple(
					"\r\nSingle user test ( {0} )\r\n\r\n",
					elapsed.ToString());
			}

			this.DisconnectDatabase();
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void table_scan() 
		{
			try
			{	
				this.testResult = this.ExecuteReader(
					"select * from uniques where col_int = 1");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		#endregion

		#region · Single User Test - Joins ·

		/* AS3AP - An ANSI SQL Standard Scalable and Portable Benchmark for Relational Database Systems
		 * 
		 * Joins (Table 7.3)
		 */

		public abstract void join_2_cl();

		public abstract void join_2_ncl();

		public abstract void join_2();		

		public abstract void join_3_cl();

		public abstract void join_3_ncl();

		public abstract void join_4_cl();

		public abstract void join_4_ncl();

		public abstract void join_1_10();

		#endregion

		#region · Single User Tests - Selections ·

		/* AS3AP - An ANSI SQL Standard Scalable and Portable Benchmark for Relational Database Systems
		 * 
		 * Selections (Table 7.2)
		 */

		/// <summary>
		/// select 1 tuple using clustered index
		/// </summary>
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_1_cl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_key = 1000");

				if (count != 1)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// select 1 tuple using secondary hashed index
		/// </summary>
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_1_ncl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_code = 'BENCHMARKS'");				

				if (count != 1)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// select 10% tuples using clustered index
		/// </summary>
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_10pct_cl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select col_key, col_int, col_signed, col_code, col_double, col_name " +
					"from uniques " +
					"where col_key <= 100000000");

				if (count != (this.tupleCount*10/100))
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// select 100 tuples using clustered index
		/// </summary>
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_100_cl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_key <= 100");

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// select 100 tuples using B-tree secondary index
		/// </summary>
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void sel_100_ncl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from updates where col_int <= 100");

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// select 10% tuples using B-tree secondary index
		/// </summary>
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	sel_10pct_ncl() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from tenpct "										+
					"where col_name = 'THE+ASAP+BENCHMARKS+'");

				if (count != (this.tupleCount*10/100))
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// range select predicated on program variable
		/// </summary>
		public void sel_variable_select_high() 
		{
			this.sel_variable_select(-250000000);
		}

		public void sel_variable_select_low() 
		{
			this.sel_variable_select(-500000000);
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	sel_variable_select(long foo) 
		{
			try
			{
				this.testResult = this.ExecuteReader(
					"select col_key, col_int, col_signed, col_code, "	+
					"col_double, col_name "								+
					"from tenpct "										+
					"where col_signed < {0}",							+
					foo);
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		#endregion

		#region · Single User Tests - Projections ·

		/* AS3AP - An ANSI SQL Standard Scalable and Portable Benchmark for Relational Database Systems
		 * 
		 * Projections (Table 7.4)
		 */

		/// <summary>
		/// project on address and signed attr.
		/// </summary>
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void proj_100() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select distinct col_address, col_signed from hundred");

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// project on signed attr.
		/// </summary>
		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void proj_10pct() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select distinct col_signed from tenpct");

				if (count != (this.tupleCount*10/100))
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		#endregion

		#region · Single User Tests - Aggregates ·

		/* AS3AP - An ANSI SQL Standard Scalable and Portable Benchmark for Relational Database Systems
		 * 
		 * Aggregates (Table 7.5)
		 */

		/// <summary>
		/// minimum key
		/// </summary>
		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_scal() 
		{	
			try
			{
				this.testResult = this.ExecuteScalar(
					"select min(col_key) from uniques");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// minimum key grouped by name
		/// </summary>
		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_func() 
		{
			try
			{
				int count = this.ExecuteReader(
					"select min(col_key) from hundred group by col_name");

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// select w/ complex predicate, then min(key)
		/// </summary>
		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_info_retrieval() 
		{
			try
			{
				this.testResult = this.ExecuteScalar(
					"select count(col_key) "									+
					"from tenpct "												+
					"where col_name = 'THE+ASAP+BENCHMARKS+' "					+
						"and col_int <= 100000000 "								+
						"and col_signed between 1 and 99999999 "				+
						"and not (col_float between -450000000 and 450000000) "	+
						"and col_double > 600000000 "							+
						"and col_decim < -600000000");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// select avg(x) where x in (select 10%)
		/// </summary>
		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_simple_report()
		{
			try
			{
				this.testResult = this.ExecuteScalar(
					"select avg(updates.col_decim) " +
					"from updates " +
					"where updates.col_key in " +
						"(select updates.col_key " +
						"from updates, hundred " +
						"where hundred.col_key = updates.col_key " +
							"and updates.col_decim > 980000000)");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_create_view() 
		{
			try
			{
				int count = this.ExecuteNonQuery(
					"create view reportview("						+
						"col_key,col_signed,col_date,col_decim, "	+
						"col_name,col_code,col_int) as "			+
					"select updates.col_key, updates.col_signed, "	+
						"updates.col_date, updates.col_decim, "		+
						"hundred.col_name, hundred.col_code, "		+
						"hundred.col_int "							+
						"from updates, hundred "					+
						"where updates.col_key = hundred.col_key");
			}
			catch (Exception)
			{				
				this.testFailed = true;
			}

			this.testResult = 0; 
		}

		/// <summary>
		/// 10% select on view, min(a),max(a),avg(a),count(b),group by code,int
		/// </summary>
		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_subtotal_report() 
		{
			try
			{
				this.testResult = this.ExecuteReader(
					"select avg(col_signed), min(col_signed), max(col_signed), "	+
					"max(col_date), min(col_date), "								+
					"count(distinct col_name), count(col_name), "					+
					"col_code, col_int "											+
					"from reportview "												+
					"where col_decim > 980000000 "									+
					"group by col_code, col_int");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		/// <summary>
		/// report 10% select on view,min(a),max(a),avg(a),count(b)
		/// </summary>
		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void agg_total_report() 
		{			
			try
			{
				this.testResult = this.ExecuteScalar(
					"select avg(col_signed), min(col_signed), max(col_signed), "	+
					"max(col_date), min(col_date), "								+
					"count(distinct col_name), count(col_name), "					+
					"count(col_code), count(col_int) "								+
					"from reportview "												+
					"where col_decim > 980000000");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		#endregion

		#region · Single User Tests - Updates ·

		/* AS3AP - An ANSI SQL Standard Scalable and Portable Benchmark for Relational Database Systems
		 * 
		 * Updates (Table 7.6)
		 */

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_append_duplicate() 
		{
			if (this.Configuration.UseIndexes) 
			{
				try
				{
					/* try to append duplicate key value */
					this.testResult = this.ExecuteNonQuery( 
						"insert into updates  "					+
						"values (6000, 0, 60000, 39997.90, "	+
						"50005.00, 50005.00, "					+
						"'11/10/1985', 'CONTROLLER', "			+
						"'ALICE IN WONDERLAND', "				+
						"'UNIVERSITY OF ILLINOIS AT CHICAGO')"); 

					this.testFailed = true;
				}
				catch (Exception)
				{
				}
			}
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_remove_duplicate() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery(
					"delete from updates where col_key = 6000 and col_int = 0");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void integrity_temp() 
		{
			try
			{
				/* Test of referential integrity:
				 * 
				 * make temp relation for restore 
				 */
				this.CreateTable("integrity_temp", this.baseTableStructure, null);

				int count = this.ExecuteNonQuery(
					"insert into integrity_temp select * from hundred where col_int = 0");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void integrity_test() 
		{
			try
			{
				int count = this.ExecuteNonQuery(
					"update hundred set col_signed = '-500000000' where col_int = 0");

				this.testFailed = true;				
				this.testResult = 0;	
			}
			catch (Exception)
			{
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void integrity_restore() 
		{
			/* restore hundred relation in case test failed 
			 */
			try
			{
				int count = this.ExecuteNonQuery(
					"delete from hundred where col_int = 0");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			try
			{
				int count = this.ExecuteNonQuery(
					"insert into hundred select * from integrity_temp");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			try
			{
				this.ExecuteNonQuery("drop table integrity_temp");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_app_t_mid() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery(
					"insert into updates "								+
					"values (5005, 5005, 50005, 50005.00, 50005.00, "	+
					"50005.00, '1/1/1988', 'CONTROLLER', "				+
					"'ALICE IN WONDERLAND', "							+
					"'UNIVERSITY OF ILLINOIS AT CHICAGO')");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_mid() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery(
					"update updates set col_key = '-5000' where col_key = 5005");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_del_t_mid() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery(
					"delete from updates where col_key = -5000 or col_key = 5005");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_app_t_end() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery(
					"insert into updates "							+
					"values (1000000001, 50005, 50005, 50005.00, "	+
					"50005.00, 50005.00, '1/1/1988', "				+
					"'CONTROLLER', 'ALICE IN WONDERLAND', "			+
					"'UNIVERSITY OF ILLINOIS AT CHICAGO')");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_end() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery(
					"update updates "	+
					"set col_key = -1000 where col_key = 1000000001");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void upd_del_t_end() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery(
					"delete from updates where col_key = -1000");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_int() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery(
					"update updates set col_int = 50015 where col_key = 5005");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void	upd_mod_t_cod() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery(
					"update updates "				+
					"set col_code = 'SQL+GROUPS' "	+
					"where col_key = 5005");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		#endregion

		#region · Single User Tests - Bulk Updates ·

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_save()
		{
			try
			{
				this.CreateTable("saveupdates", baseTableStructure, null);

				int count = this.ExecuteNonQuery(
					"insert into saveupdates select * "	+
					"from updates where col_key between 5000 and 5999");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}
	
		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_append() 
		{
			try
			{
				int count = this.ExecuteNonQuery(
					"insert into updates select * from saveupdates");

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void bulk_modify() 
		{
			try
			{
				int count = this.ExecuteNonQuery(
					"update updates "					+
					"set col_key = col_key - 100000 "	+
					"where col_key between 5000 and 5999");
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
				int count = this.ExecuteNonQuery(
					"delete from updates where col_key < 0");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		#endregion

		#region · Multi User Tests - Main ·

		public void MultiUserTests(int nInstances) 
		{	
			TimeSpan fTime;
			DateTime sTime;

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
					new ProgressMessageEventArgs(
					"Run IR (Mix 1) test for {0} minutes ({1})",
					this.timeToRun,
					DateTime.Now.ToString()));
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
					new ProgressMessageEventArgs(
					"Run Measure throughput in IR test for {0} minutes ({1}).",
					this.timeToRun,
					DateTime.Now.ToString()));
			}
			
			sTime			= DateTime.Now;
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

			fTime = DateTime.Now - sTime;

			if (this.log != null)
			{
				this.log.Simple(
					"Mixed IR (tup/sec)\t{0}\t returned in {1} minutes",
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
					new ProgressMessageEventArgs(
					"Run Mixed Workload IR test (Mix 3) ({0}).",
					DateTime.Now.ToString()));
			}
			
			this.userProcess[0]	= new Thread(new ThreadStart(this.cross_section_tests));

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
					new ProgressMessageEventArgs(
					"Check correctness of the sequential and random bulk updates ({0}).",
					DateTime.Now.ToString()));
			}
			
			this.ConnectDatabase();

			this.runTest("mu_checkmod_100_seq");
			this.runTest("mu_checkmod_100_rand");

			this.DisconnectDatabase();
			
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
					new ProgressMessageEventArgs(
					"Check correctness of the sequential and random bulk updates ({0}).",
					DateTime.Now.ToString()));
			}
			
			this.ConnectDatabase();

			this.runTest("mu_checkmod_100_seq");
			this.runTest("mu_checkmod_100_rand");

			this.runTest("mu_drop_sel100_seq");
			this.runTest("mu_drop_sel100_rand");

			this.DisconnectDatabase();

			/* Step 8 - Run OLTP test for 15 minutes.	*/
			if (this.Progress != null)
			{
				this.Progress(
					this, 
					new ProgressMessageEventArgs(
					"Run OLTP test for {0} minutes ({1})",
					this.timeToRun,
					DateTime.Now.ToString()));
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
					new ProgressMessageEventArgs(
					"Run Measure throughput in IR test for {0} minutes ({1}).",
					this.timeToRun,
					DateTime.Now.ToString()));
			}

			sTime			= DateTime.Now;
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
						
			fTime = DateTime.Now - sTime;
			
			if (this.log != null)
			{
				this.log.Simple(
					"Mixed OLTP (tup/sec)\t{0}\t returned in {1} minutes\n",
					Math.Round((double)iters/fTime.TotalSeconds, 4), 
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
					new ProgressMessageEventArgs(
					"Run Mixed Workload OLTP test (Mix 4) ({0}).",
					DateTime.Now.ToString()));
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
					new ProgressMessageEventArgs(
					"Check correctness of the sequential and random bulk updates ({0}).",
					DateTime.Now.ToString()));
			}
			
			this.ConnectDatabase();

			this.runTest("mu_checkmod_100_seq");			
			this.runTest("mu_checkmod_100_rand");
			
			this.runTest("mu_drop_sel100_seq");
			this.runTest("mu_drop_sel100_rand");

			this.DisconnectDatabase();
		}

		private void oltp_update()
		{	
			try
			{
				ITestSuite testSuite = TestSuiteFactory.GetTestSuite(
					testSuiteName, configuration);
			
				testSuite.ConnectDatabase();
			
				if (timeToRun > 0)
				{
					DateTime endTime = DateTime.Now.AddMinutes(timeToRun);

					while (endTime >= DateTime.Now)
					{
						testSuite.mu_oltp_update();
					}
				}
				else
				{
					testSuite.mu_oltp_update();
				}
			
				testSuite.DisconnectDatabase();
			}
			catch (ThreadAbortException)
			{
			}
		}

		private void ir_select()
		{	
			try
			{
				ITestSuite testSuite = TestSuiteFactory.GetTestSuite(
					testSuiteName, configuration);

				testSuite.ConnectDatabase();
							
				if (timeToRun > 0)
				{
					DateTime endTime = DateTime.Now.AddMinutes(timeToRun);

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
			
				testSuite.DisconnectDatabase();
			}
			catch (ThreadAbortException)
			{
			}
		}

		#endregion

		#region · Cross Section Tests Main ·

		private void cross_section_tests() 
		{
			try
			{
				DateTime startTime;

				this.ConnectDatabase();

				startTime = DateTime.Now;

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

				TimeSpan elapsed = startTime - DateTime.Now;

				this.DisconnectDatabase();

				if (this.log != null)
				{
					this.log.Simple("CrossSectionTests \t( {0} )", elapsed.ToString());
				}
			}
			catch (ThreadAbortException)
			{
			}
		}

		#endregion

		#region · Multi User Tests - Cross Section Tests for Mixed IR and Mixed OLTP tests ·

		[IsolationLevel(System.Data.IsolationLevel.ReadCommitted)]
		public void mu_oltp_update()
		{
			int		r			= 0;
			Random	randNumber	= new Random(unchecked((int)DateTime.Now.Ticks));
			string	sql			= 
				"update updates set col_signed = col_signed + 1 " +
				"where col_key = {0}";

			try
			{				
				r = 1;
				while (r == 1)
				{
					r = randNumber.Next(0, tupleCount);   // There IS no col_key 1
				}	

				this.testResult = this.ExecuteNonQuery(sql, r);
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_ir_select()
		{			
			Random	randNumber	= new Random(unchecked((int)DateTime.Now.Ticks));
			int		r			= 0;
			string	sql			=
				"select col_key, col_code, col_date, col_signed, col_name "	+
				"from updates where col_key = {0}";

			try
			{
				while (r == 1)
				{
					r = randNumber.Next(0, tupleCount);   // there IS no key 1
				}
		
				this.testResult = this.ExecuteReader(sql, r);
			}	
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void o_mode_tiny()
		{	
			try
			{
				this.testResult = this.ExecuteReader("select * from tiny");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void o_mode_100k()
		{
			string sql = "select * from hundred where col_key <= 1000";

			try
			{
				this.testResult = this.ExecuteReader(sql);
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_checkmod_100_rand() 
		{
			string sql = 
					"select count(*) from updates, sel100rand "+
					"where updates.col_int = sel100rand.col_int "		+
					"and not updates.col_double = sel100rand.col_double";

			try
			{
				int count = this.ExecuteReader(sql);

				if (count != 100) 
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_drop_sel100_rand() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery("drop table sel100rand");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_checkmod_100_seq() 
		{
			string sql = "select count(*) from updates, sel100seq "		+
						"where updates.col_key = sel100seq.col_key "	+
						"and not updates.col_double = sel100seq.col_double";

			try
			{
				int count = this.ExecuteReader(sql);

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void mu_drop_sel100_seq() 
		{
			try
			{
				this.testResult = this.ExecuteNonQuery("drop table sel100seq");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_mod_100_rand() 
		{
			try
			{
				int count = this.ExecuteNonQuery(
					"update updates "							+
					"set col_double = col_double + 100000000 "	+
					"where col_int between 1001 and 1100");

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_mod_100_seq() 
		{
			try
			{
				this.testResult = this.ExecuteAborting(
					"update updates "							+
					"set col_double = col_double + 100000000 "	+
					"where col_key between 1001 and 1100");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_sel_100_rand() 
		{
			try
			{
				this.CreateTable("sel100rand", baseTableStructure, null);

				int count = this.ExecuteNonQuery(
					"insert into sel100rand select * from updates "	+
					"where updates.col_int between 1001 and 1100");

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_sel_100_seq() 
		{
			try
			{
				this.CreateTable("sel100seq", baseTableStructure, null);

				int count = this.ExecuteNonQuery(
					"insert into sel100seq select * from updates "	+
					"where updates.col_key between 1001 and 1100");

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_unmod_100_rand() 
		{
			try
			{
				int count = this.ExecuteNonQuery(
					"update updates "	+
					"set col_double = col_double - 100000000 "	+
					"where col_int between 1001 and 1100");

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		[IsolationLevel(IsolationLevel.RepeatableRead)]
		public void mu_unmod_100_seq() 
		{
			try
			{
				int count = this.ExecuteNonQuery(
					"update updates "							+
					"set col_double = col_double - 100000000 "	+
					"where col_key between 1001 and 1100");

				if (count != 100)
				{
					this.testFailed = true;
				}

				this.testResult = count;
			}
			catch (Exception)
			{
				this.testFailed = true;
			}
		}

		#endregion

		#region · Table and Index handling ·

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_tables() 
		{
			try
			{
				this.CreateTable("uniques", this.baseTableStructure, "col_key");
				this.CreateTable("hundred", this.baseTableStructure, "col_key");
				this.CreateTable("updates", this.baseTableStructure, "col_key");
				this.CreateTable("tenpct" , this.baseTableStructure, "col_key,col_code");

				this.CreateTable( 
					"tiny"					,
					"col_key " + this.Configuration.IntegerTypeName + " not null",
					"col_key");
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
			try
			{
				this.CreateIndex(IndexType.Btree, "hundred_code_h", "hundred", "col_code");
			}
			catch (Exception)
			{
				this.testFailed = true;
			}

			this.testResult = 0;
		}

		[IsolationLevel(IsolationLevel.ReadCommitted)]
		public void create_idx_hundred_foreign() 
		{
			if (this.Configuration.UseIndexes) 
			{  
				try
				{
					this.CreateForeignKey("hundred", "fk_hundred_updates", "col_signed", "updates", "col_key");
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
			if (this.Configuration.UseIndexes && 
				this.Configuration.SupportsClusteredIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Clustered, "hundred_key_bt", "hundred", "col_key");
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
			if (this.Configuration.UseIndexes && this.Configuration.SupportsHashIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Hash	,"tenpct_code_h", "tenpct", "col_code");
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
			if (this.Configuration.UseIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Btree, "tenpct_decim_bt", "tenpct", "col_decim");
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
			if (this.Configuration.UseIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Btree, "tenpct_double_bt", "tenpct", "col_double");
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
			if (this.Configuration.UseIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Btree, "tenpct_float_bt", "tenpct", "col_float");
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
			if (this.Configuration.UseIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Btree, "tenpct_int_bt", "tenpct", "col_int");
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
			if (this.Configuration.UseIndexes && this.Configuration.SupportsClusteredIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Clustered, "tenpct_key_bt", "tenpct", "col_key");
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
			if (this.Configuration.UseIndexes)
			{
				try
				{
					this.CreateIndex(IndexType.Btree, "tenpct_key_code_bt", "tenpct", "col_key, col_code");
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
			if (this.Configuration.UseIndexes && this.Configuration.SupportsHashIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Hash, "tenpct_name_h", "tenpct", "col_name");
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
			if (this.Configuration.UseIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Btree, "tenpct_signed_bt", "tenpct", "col_signed");
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
			if (this.Configuration.UseIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Btree, "tiny_key_bt", "tiny", "col_key");
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
			if (this.Configuration.UseIndexes && this.Configuration.SupportsHashIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Hash, "uniques_code_h", "uniques", "col_code");
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
			if (this.Configuration.UseIndexes && 
				this.Configuration.SupportsClusteredIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Clustered, "uniques_key_bt", "uniques", "col_key");
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
			if (this.Configuration.UseIndexes && this.Configuration.SupportsHashIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Hash, "updates_code_h", "updates", "col_code");
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
			if (this.Configuration.UseIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Btree, "updates_decim_bt", "updates", "col_decim");
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
			if (this.Configuration.UseIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Btree, "updates_double_bt", "updates", "col_double");
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
			if (this.Configuration.UseIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Btree, "updates_int_bt", "updates", "col_int");
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
			if (this.Configuration.UseIndexes && this.Configuration.SupportsClusteredIndexes) 
			{
				try
				{
					this.CreateIndex(IndexType.Clustered, "updates_key_bt", "updates", "col_key");
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
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
					this.ExecuteNonQuery("drop index updates_int_bt");
					this.ExecuteNonQuery("drop index updates_double_bt");
					this.ExecuteNonQuery("drop index updates_decim_bt");
					if (this.Configuration.SupportsHashIndexes)
					{
						this.ExecuteNonQuery("drop index updates_code_h");
					}
				}
				catch (Exception)
				{
					this.testFailed = true;
				}
			}

			this.testResult = 0;
		}

		#endregion
		
		#region · Database Creation Methods ·

		private void DropDatabase()
		{
            try
            {
                string catalog = ""; 
                string connectionString = ConfigurationManager.ConnectionStrings[this.configuration.ConnectionStringName].ConnectionString;
                string providerName = ConfigurationManager.ConnectionStrings[this.configuration.ConnectionStringName].ProviderName;

                if (providerName.ToLower().Trim() == "firebirdsql.data.firebirdclient")
                {
                    FirebirdSql.Data.FirebirdClient.FbConnection.DropDatabase(connectionString);
                }
                else
                {
                    DbConnectionStringBuilder builder = this.providerFactory.CreateConnectionStringBuilder();

                    builder.ConnectionString = connectionString;

                    if (builder.ContainsKey("Catalog"))
                    {
                        catalog = builder["Catalog"].ToString();
                        builder["Catalog"] = "";
                    }
                    else if (builder.ContainsKey("Database"))
                    {
                        catalog = builder["Database"].ToString();
                        builder["Database"] = "";
                    }

                    DbConnection connection = this.providerFactory.CreateConnection();
                    connection.ConnectionString = builder.ToString();
                    connection.Open();

                    DbCommand command = connection.CreateCommand();

                    try
                    {
                        command.CommandText = String.Format(this.Configuration.DropDatabaseStmt, catalog);
                        command.ExecuteNonQuery();
                    }
                    catch
                    {
                    }
                    finally
                    {
                        connection.Close();
                    }
                }
            }
            catch (Exception)
            {
            }
		}

		private void CreateIndex(IndexType indextype, string indexName, string tableName, string fields)
		{
			if (this.Configuration.UseIndexes) 
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
					this.ExecuteNonQuery(createIndexStmt);
				}
				catch (Exception ex)
				{
					if (this.log != null)
					{
						this.log.Error("btree error {0}", ex.Message);
					}
					throw ex;				
				}
			}
		}

		private void CreateForeignKey(
			string foreignTable, 
			string constraintName, 
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
				this.ExecuteNonQuery(commandText.ToString());
			}
			catch (Exception ex)
			{
				if (this.log != null)
				{
					this.log.Error("foreign key error {0}", ex.Message);
				}
				throw ex;
			}
		}

		private void CreateTable(string tableName, string tableStructure, string primaryKey) 
		{
			StringBuilder commandText = new StringBuilder();

			try			
			{
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

				this.ExecuteNonQuery(commandText.ToString());
			}
			catch (Exception ex)
			{
				if (this.log != null)
				{
					this.log.Error("error create table {0}", ex.Message);
				}
				throw ex;
			}
		}

		public void ConnectDatabase()
		{
			try
			{
                this.connection = this.providerFactory.CreateConnection();
                this.connection.ConnectionString = ConfigurationManager.ConnectionStrings[this.Configuration.ConnectionStringName].ConnectionString;
				this.connection.Open();
			}
			catch (Exception ex)
			{
				if (this.log != null) 
				{
					this.log.Error("ConnectDatabase error {0}", ex.Message);
				}
				throw ex;
			}
		}

		public void DisconnectDatabase()
		{
			try
			{
				if (this.connection != null)
				{
					this.connection.Close();
					this.connection	= null;
				}
			}
			catch (Exception ex)
			{
				if (this.log != null)
				{
					this.log.Error("disconnect error {0}", ex.Message);
				}
				throw ex;
			}
		}

		protected int ExecuteNonQuery(string format, params object[] args)
		{
			StringBuilder b = new StringBuilder();

			b.AppendFormat(format, args);

			return this.ExecuteNonQuery(b.ToString());    			
		}

		protected int ExecuteNonQuery(string commandText)
		{
			DbCommand		command		= null;
			DbTransaction	transaction = null;
			int				count		= 0;

			try
			{
				transaction = this.BeginTransaction();
				command		= this.CreateCommand(commandText, transaction);
				count		= command.ExecuteNonQuery();

				transaction.Commit();
			}
			catch (Exception ex)
			{
				if (transaction != null)
				{
					transaction.Rollback();
					transaction = null;
				}
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

			return count;
		}

		protected int ExecuteAborting(string format, params object[] args)
		{
			StringBuilder b = new StringBuilder();

			b.AppendFormat(format, args);

			return this.ExecuteAborting(b.ToString());    			
		}

		protected int ExecuteAborting(string commandText)
		{
			DbCommand		command		= null;
			DbTransaction	transaction = null;
			int				count		= 0;

			try
			{
				transaction = this.BeginTransaction();
				command		= this.CreateCommand(commandText, transaction);
				count		= command.ExecuteNonQuery();
			}
			catch (Exception ex)
			{
				throw ex;
			}
			finally
			{	
				if (transaction != null)
				{
					transaction.Rollback();
					transaction = null;
				}
				if (command != null)
				{
					command.Dispose();
					command = null;	
				}
			}

			return count;
		}

		protected int ExecuteReader(string format, params object[] args)
		{
			StringBuilder b = new StringBuilder();

			b.AppendFormat(format, args);

			return this.ExecuteReader(b.ToString());    			
		}

		protected int ExecuteReader(string commandText)
		{
			DbCommand		command		= null;
			IDataReader		cursor		= null;
			DbTransaction	transaction = null;
			int				count		= 0;

			try
			{
				transaction = this.BeginTransaction();
				command		= this.CreateCommand(commandText, transaction);
				cursor		= command.ExecuteReader();

				while (cursor.Read())
				{
					count++;
				}
				
				cursor.Close();
				transaction.Commit();
			}
			catch (Exception ex)
			{
				if (cursor != null)
				{
					cursor.Close();
					cursor = null;
				}
				if (transaction != null)
				{
					transaction.Rollback();
					transaction = null;
				}
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

			return count;
		}

		protected object ExecuteScalar(string format, params object[] args)
		{
			StringBuilder b = new StringBuilder();

			b.AppendFormat(format, args);

			return this.ExecuteScalar(b.ToString());    			
		}

		protected object ExecuteScalar(string commandText)
		{
			DbCommand		command		= null;
			DbTransaction	transaction = null;
			object			result		= null;

			try
			{
				transaction = this.BeginTransaction();
				command		= this.CreateCommand(commandText, transaction);
				result		= command.ExecuteScalar();
				
				transaction.Commit();
			}
			catch (Exception ex)
			{
				if (transaction != null)
				{
					transaction.Rollback();
					transaction = null;
				}
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

			return result;
		}

		protected DbTransaction BeginTransaction()
		{
			try
			{
                return (DbTransaction)this.connection.BeginTransaction(isolation);
			}
			catch (Exception ex)
			{
				if (this.log != null)
				{
					this.log.Error("this.beginTransaction failed {0}", ex.Message);
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
					this.Result(this, new TestResultEventArgs("	tiny file loaded", 0, DateTime.Now - start, false));
				}

				start = DateTime.Now;
								
				this.LoadFile("uniques");

				if (this.Result != null)
				{
					this.Result(this, new TestResultEventArgs("	uniques file loaded", 0, DateTime.Now - start, false));
				}

				start = DateTime.Now;
				
				this.LoadFile("updates");

				if (this.Result != null)
				{
					this.Result(this, new TestResultEventArgs("	updates file loaded", 0, DateTime.Now - start, false));
				}

				start = DateTime.Now;
				
				this.LoadFile("hundred");

				if (this.Result != null)
				{
					this.Result(this, new TestResultEventArgs("	hundred file loaded", 0, DateTime.Now - start, false));
				}

				start = DateTime.Now;
				
				this.LoadFile("tenpct");

				if (this.Result != null)
				{
					this.Result(this, new TestResultEventArgs("	tenpct file loaded", 0, DateTime.Now - start, false));
				}
			}
			catch (Exception ex)
			{
				if (this.log != null) 
				{
					this.log.Error("load failed {0}", ex.Message);
				}
				
				throw ex;
			}
		}

		private void LoadFile(string table)
		{
			StringBuilder	commandText			= new StringBuilder();
			StreamReader	stream				= null;
			DbCommand		command				= null;			
			DbTransaction	transaction			= null;
			bool			transactionPending	= false;
			bool			commandPrepared		= false;
			int				rowCount			= 0;			
			
			commandText.AppendFormat("insert into {0} values (@col_key,@col_int,@col_signed,@col_float,@col_double,@col_decim,@col_date,@col_code,@col_name,@col_address)", table);
			
			string path = Path.GetFullPath(configuration.DataPath);
			if (!path.EndsWith(Path.DirectorySeparatorChar.ToString()))
			{
				path += Path.DirectorySeparatorChar;
			}

			if (!File.Exists(path + "asap." + table))
			{
				throw new FileNotFoundException("AS3AP data file not found", path + "asap." + table);
			}

			stream = new StreamReader(
				new BufferedStream((System.IO.Stream)File.Open(
				path + "asap." + table	,
				FileMode.Open			,
				FileAccess.Read			,
				FileShare.None)));

			/* Crate command */
			command = this.CreateCommand(commandText.ToString(), transaction);

			/* Add parameters	*/
			command.Parameters.Add(this.CreateParameter("@col_key", DbType.Int32, 4, "col_key"));
			command.Parameters.Add(this.CreateParameter("@col_int", DbType.Int32, 4, "col_int"));
			command.Parameters.Add(this.CreateParameter("@col_signed", DbType.Int32, 4, "col_signed"));
			command.Parameters.Add(this.CreateParameter("@col_float", DbType.Single, 4, "col_float"));
			command.Parameters.Add(this.CreateParameter("@col_double", DbType.Double, 8, "col_double"));
			command.Parameters.Add(this.CreateParameter("@col_decim", DbType.Single, 18, "col_decim"));
			command.Parameters.Add(this.CreateParameter("@col_date", DbType.StringFixedLength, 20, "col_date"));
			command.Parameters.Add(this.CreateParameter("@col_code", DbType.StringFixedLength, 10, "col_code"));
			command.Parameters.Add(this.CreateParameter("@col_name", DbType.StringFixedLength, 20, "col_name"));
			command.Parameters.Add(this.CreateParameter("@col_address", DbType.String, 80, "col_address"));

			while (stream.Peek() > -1)
			{
				if (rowCount == 0)
				{
					transaction			= this.BeginTransaction();
					transactionPending	= true;
					command.Transaction = transaction;

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
					((DbParameter)command.Parameters[i]).Value = elements[i];
				}

				command.ExecuteNonQuery();

				rowCount++;

				if (rowCount >= 1000)
				{
					transaction.Commit();
					transactionPending	= false;
					rowCount			= 0;
				}
			}

			if (transactionPending)
			{
				transaction.Commit();
			}

			command.Dispose();
			stream.Close();
		}

		private void loadTinyFile(string table)
		{
			StringBuilder	commandText = new StringBuilder();
			StreamReader	stream		= null;
			DbCommand		command		= null;
			DbTransaction	transaction	= null;

			commandText.AppendFormat("insert into {0} values (@col_key)", table);

			string path = Path.GetFullPath(configuration.DataPath);
			if (!path.EndsWith(Path.DirectorySeparatorChar.ToString()))
			{
				path += Path.DirectorySeparatorChar;
			}

			if (!File.Exists(path + "asap." + table))
			{
				throw new FileNotFoundException("AS3AP data file not found", path + "asap." + table);
			}

			stream = new StreamReader(
				new BufferedStream((System.IO.Stream)File.Open(
				path + "asap." + table	,
				FileMode.Open			,
				FileAccess.Read			,
				FileShare.None)));

			/* Crate command */
			transaction	= this.BeginTransaction();
			command		= this.CreateCommand(commandText.ToString(), transaction);

			/* Add parameters	*/
			command.Parameters.Add(this.CreateParameter("@col_key", DbType.Int32, 4, "col_key"));

			/* Prepare command execution	*/
			command.Prepare();

			while (stream.Peek() > -1)
			{
				string[] elements = stream.ReadLine().Split(',');
			
				((DbParameter)command.Parameters[0]).Value = elements[0];
	
				command.ExecuteNonQuery();
			}

			transaction.Commit();
			command.Dispose();
			stream.Close();
		}

		protected DbCommand CreateCommand(string commandText, DbTransaction transaction)
		{
            DbCommand command = (DbCommand)this.connection.CreateCommand();

			command.CommandText = commandText;
			command.Transaction	= transaction;

			return command;
		}

        protected DbParameter CreateParameter(string parameterName, DbType dbType, int size, string sourceColumn)
        {
            DbParameter parameter = this.providerFactory.CreateParameter();

            parameter.ParameterName = parameterName;
            parameter.DbType        = dbType;
            parameter.Size          = size;
            parameter.SourceColumn  = sourceColumn;

            return parameter;
        }

		#endregion	
	}
}
