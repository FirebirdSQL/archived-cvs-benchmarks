//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
//
// Ported from OSDB project at http://osdb.sourceforge.net
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
using System.Reflection;
using System.Configuration;

using AS3AP.LogData;

namespace AS3AP.BenchMark
{
	public class AS3AP
	{		
		#region FIELDS

		private	Logger		log;
		private	long		ticksPerSecond	= TimeSpan.TicksPerSecond;		
		private CallableSql callableSql;
		private string		currentTest		= String.Empty;

		private string		backendName		= String.Empty;

		private bool		runCreate		= true;
		private bool		runSingleUser	= true;
		private bool		runMultiUser	= false;

		private int			iters			= 0;
		private int			timeToRun		= 15;
		private int			userNumber		= 0;
		private long		dataSize		= 0;

		#endregion

		#region CONSTRUCTORS

		public AS3AP()
		{
			if (ConfigurationSettings.AppSettings["LogMode"].ToUpper() == "APPEND")
			{
				log	= new Logger(GetType(), ConfigurationSettings.AppSettings["LogFile"], Mode.APPEND);
			}
			else
			{
				log	= new Logger(GetType(), ConfigurationSettings.AppSettings["LogFile"], Mode.OVERWRITE);
			}
			getConfiguration();

			callableSql	= new CallableSql(backendName);
		}

		#endregion

		#region METHODS

		private void getConfiguration()
		{
			runCreate		= Boolean.Parse(ConfigurationSettings.AppSettings["RunCreate"]);
			runSingleUser	= Boolean.Parse(ConfigurationSettings.AppSettings["RunSingleUser"]);
			runMultiUser	= Boolean.Parse(ConfigurationSettings.AppSettings["RunMultiUser"]);

			backendName		= ConfigurationSettings.AppSettings["Backend"];

			userNumber		= Int32.Parse(ConfigurationSettings.AppSettings["UserNumber"]);
			dataSize		= Int64.Parse(ConfigurationSettings.AppSettings["DataSize"]);			
		}

		public void Run()
		{
			TimeSpan	elapsed;
			long		clocks;
			int			dbSize;
			
			currentTest = "Test Initialization";

			log.Simple("Starting as3ap benchmar at: {0}", DateTime.Now);

			/* Start of database table creation */
			if (runCreate) 
			{
				callableSql.Backend.DatabaseCreate("AS3AP");
				callableSql.Backend.DatabaseConnect();

				/* Database data generation should go here */
				Console.WriteLine("Creating tables and loading data {0}.", DateTime.Now);
				timeIt("populateDataBase()", "populateDataBase");
				
				callableSql.Backend.DatabaseDisconnect();
			}			

			currentTest = "Counting tuples";
			callableSql.Backend.DatabaseConnect();
			if ((callableSql.TupleCount = callableSql.Backend.CountTuples("updates")) == 0)
			{
				log.Simple("empty database -- empty results");
				return;
			}		
			dbSize = (4 * callableSql.TupleCount * 100)/1000000;
			/* dbSize: 4 relations, N tuples/relation, 100 bytes/tuple */
			log.Simple("");
			log.Simple("\"Logical database size {0}MB\"", dbSize);
			log.Simple("");
			log.Simple("");
			callableSql.Backend.DatabaseDisconnect();

			/* Start of the single user test */
			if (runSingleUser)
			{
				Console.WriteLine("Starting single-user test");
				currentTest = "Preparing single user test";
				clocks		= DateTime.Now.Ticks;
				singleUserTests();
				elapsed		= elapsedTime(clocks = (DateTime.Now.Ticks - clocks));
				
				log.Simple("");
				log.Simple("\"Single User Test\"\t{0} seconds\t({1})",
							(double)clocks / TimeSpan.TicksPerSecond, elapsed);
				log.Simple("");
				log.Simple("");
			}

			/* Start of the multi-user test */
			if (runMultiUser)
			{
				currentTest = "Preparing multi-user test";
				callableSql.Backend.DatabaseConnect();
				if (callableSql.TupleCount != callableSql.Backend.CountTuples("updates")) 
				{
					log.Simple("data corrupted; skipping multi-user test");					
				}
				callableSql.Backend.DatabaseDisconnect();
				Console.WriteLine("Starting multi-user test");
				
				clocks = DateTime.Now.Ticks;
				multiUserTests(userNumber == 0 ? (int)(dbSize / 4) : userNumber);
				elapsed = elapsedTime(clocks = (DateTime.Now.Ticks - clocks));

				log.Simple("");
				log.Simple("\"Multi-User Test\"\t{0} seconds\t({1})",
							(double)clocks / TimeSpan.TicksPerSecond, elapsed);
				log.Simple("");
				log.Simple("");
			}
		}


		private TimeSpan elapsedTime(long ticks)
		{
			return new TimeSpan(ticks);
		}


		private int populateDataBase() 
		{
			timeIt("create_tables()"				, "create_tables");
			timeIt("load()"							, "load", dataSize);
			timeIt("create_idx_uniques_key_bt()"	, "create_idx_uniques_key_bt");
			timeIt("create_idx_updates_key_bt()"	, "create_idx_updates_key_bt");
			timeIt("create_idx_hundred_key_bt()"	, "create_idx_hundred_key_bt");
			timeIt("create_idx_tenpct_key_bt()"		, "create_idx_tenpct_key_bt");
			timeIt("create_idx_tenpct_key_code_bt()", "create_idx_tenpct_key_code_bt");
			timeIt("create_idx_tiny_key_bt()"		, "create_idx_tiny_key_bt");
			timeIt("create_idx_tenpct_int_bt()"		, "create_idx_tenpct_int_bt");
			timeIt("create_idx_tenpct_signed_bt()"	, "create_idx_tenpct_signed_bt");
			timeIt("create_idx_uniques_code_h()"	, "create_idx_uniques_code_h");
			timeIt("create_idx_tenpct_double_bt()"	, "create_idx_tenpct_double_bt");
			timeIt("create_idx_updates_decim_bt()"	, "create_idx_updates_decim_bt");
			timeIt("create_idx_tenpct_float_bt()"	, "create_idx_tenpct_float_bt");
			timeIt("create_idx_updates_int_bt()"	, "create_idx_updates_int_bt");
			timeIt("create_idx_tenpct_decim_bt()"	, "create_idx_tenpct_decim_bt");
			timeIt("create_idx_hundred_code_h()"	, "create_idx_hundred_code_h");
			timeIt("create_idx_tenpct_name_h()"		, "create_idx_tenpct_name_h");
			timeIt("create_idx_updates_code_h()"	, "create_idx_updates_code_h");
			timeIt("create_idx_tenpct_code_h()"		, "create_idx_tenpct_code_h");
			timeIt("create_idx_updates_double_bt()"	, "create_idx_updates_double_bt");
			timeIt("create_idx_hundred_foreign()"	, "create_idx_hundred_foreign");
			
			return 0;
		}


		private void singleUserTests() 
		{
			callableSql.Backend.DatabaseConnect();
			
			timeIt("sel_1_cl()"					, "sel_1_cl");
			timeIt("join_3_cl()"				, "join_3_cl");
			timeIt("sel_100_ncl()"				, "sel_100_ncl");
			timeIt("table_scan()"				, "table_scan");
			timeIt("agg_func()"					, "agg_func");
			timeIt("agg_scal()"					, "agg_scal");
			timeIt("sel_100_cl()"				, "sel_100_cl");
			timeIt("join_3_ncl()"				, "join_3_ncl");
			timeIt("sel_10pct_ncl()"			, "sel_10pct_ncl");
			timeIt("agg_simple_report()"		, "agg_simple_report");
			timeIt("agg_info_retrieval()"		, "agg_info_retrieval");
			timeIt("agg_create_view()"			, "agg_create_view");
			timeIt("agg_subtotal_report()"		, "agg_subtotal_report");
			timeIt("agg_total_report()"			, "agg_total_report");
			timeIt("join_2_cl()"				, "join_2_cl");
			timeIt("join_2()"					, "join_2");
			timeIt("sel_variable_select_low()"	, "sel_variable_select_low");
			timeIt("sel_variable_select_high()"	, "sel_variable_select_high");
			timeIt("join_4_cl()"				, "join_4_cl");
			timeIt("proj_100()"					, "proj_100");
			timeIt("join_4_ncl()"				, "join_4_ncl");
			timeIt("proj_10pct()"				, "proj_10pct");
			timeIt("sel_1_ncl()"				, "sel_1_ncl", IsolationLevel.ReadCommitted);
			timeIt("join_2_ncl()"				, "join_2_ncl");
			timeIt("integrity_test()"			, "integrity_test");
			timeIt("drop_updates_keys()"		, "drop_updates_keys");
			timeIt("bulk_save()"				, "bulk_save");
			timeIt("bulk_modify()"				, "bulk_modify");
			timeIt("upd_append_duplicate()"		, "upd_append_duplicate");
			timeIt("upd_remove_duplicate()"		, "upd_remove_duplicate");
			timeIt("upd_app_t_mid()"			, "upd_app_t_mid");
			timeIt("upd_mod_t_mid()"			, "upd_mod_t_mid");
			timeIt("upd_del_t_mid()"			, "upd_del_t_mid");
			timeIt("upd_app_t_end()"			, "upd_app_t_end");
			timeIt("upd_mod_t_end()"			, "upd_mod_t_end");
			timeIt("upd_del_t_end()"			, "upd_del_t_end");
			timeIt("create_idx_updates_code_h()", "create_idx_updates_code_h");
			timeIt("upd_app_t_mid()"			, "upd_app_t_mid");
			timeIt("upd_mod_t_cod()"			, "upd_mod_t_cod");
			timeIt("upd_del_t_mid()"			, "upd_del_t_mid");
			timeIt("create_idx_updates_int_bt()", "create_idx_updates_int_bt");
			timeIt("upd_app_t_mid()"			, "upd_app_t_mid");
			timeIt("upd_mod_t_int()"			, "upd_mod_t_int");
			timeIt("upd_del_t_mid()"			, "upd_del_t_mid");
			timeIt("bulk_append()"				, "bulk_append");
			timeIt("bulk_delete()"				, "bulk_delete");

			callableSql.Backend.DatabaseDisconnect();
		}


		private void multiUserTests(int nInstances) 
		{	
			double		fTime;
			DateTime	startTime;
			Thread[]	process = new Thread[nInstances];
			
			currentTest = "Multi-user tests";			

			log.Simple("\"Executing multi-user tests with {0} user task{1}\"\n",
						nInstances, ((nInstances != 1) ? "s" : ""));

			/* Step 1 -- Backup updates relation, including indices, 
			 * to tape or other device. This is done early on.
			 */
			
			/* Step 2 -- Run IR (Mix 1) test for 15 minutes.	*/
			Console.WriteLine("Run IR (Mix 1) test for 15 minutes ({0}).", DateTime.Now);
			iters		= 0;
			timeToRun	= 15;
			for (int i = 0; i < nInstances; i++) 
			{
				process[i]		= new Thread(new ThreadStart(ir_select));
				process[i].Name = "User " + i.ToString();
				process[i].Start();
			}
			
			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				process[i].Join();
			}
						
			/* Step 3 -- Measure throughput in IR test for five minutes.	*/
			Console.WriteLine("Run Measure throughput in IR test for five minutes ({0}).", DateTime.Now);
			iters		= 0;
			startTime	= DateTime.Now;
			timeToRun	= 5;
			for (int i = 0; i < nInstances; i++) 
			{
				process[i] = new Thread(new ThreadStart(ir_select));
				process[i].Name = "User " + i.ToString();
				process[i].Start();
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				process[i].Join();
			}

			fTime = ((double)(DateTime.Now.Ticks - startTime.Ticks)) / ticksPerSecond;
			log.Simple("Mixed IR (tup/sec)\t{0}"			+
						"\treturned in {1} minutes"			,
						Math.Round((double)iters/fTime, 4)	, 
						Math.Round(fTime/60, 4));

			/* Step 4 -- A Mixed Workload IR Test, where one user executes a cross 
			 * section of ten update and retrieval queries, and all the others 
			 * execute the same IR query as in the second test.
			 */
			Console.WriteLine("Run Mixed Workload IR test (Mix 3). ({0}).", DateTime.Now);
			currentTest		= "Mixed IR";
			process[0]		= new Thread(new ThreadStart(crossSectionTests));
			process[0].Name = "User " + 0.ToString();
			process[0].Start();
			/* Exec the only one time in each thread	*/
			timeToRun	= -1;
			for (int i = 1; i < nInstances; i++) 
			{
				process[i] = new Thread(new ThreadStart(ir_select));
				process[i].Name = "User " + i.ToString();
				process[i].Start();
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				process[i].Join();
			}
			
			/* Step 5 -- Run queries to check correctness of the sequential 
			 * and random bulk updates.
			 */
			Console.WriteLine("Check correctness of the sequential and random bulk updates ({0}).", DateTime.Now);
			callableSql.Backend.DatabaseConnect();
			timeIt("mu_checkmod_100_seq()"	, "mu_checkmod_100_seq");
			timeIt("mu_checkmod_100_rand()"	, "mu_checkmod_100_rand");
			callableSql.Backend.DatabaseDisconnect();

			/* Step 6 - Recover updates relation from backup tape (Step 1) 
			 * and log (from Steps 2, 3, 4, and 5).	
			 */


			/* Step 7 - Perform correctness checks, checkmod_100_seq and 
			 * checkmod_100_rand. Remove temporary tables: sel100seq and 
			 * sel100rand.
			 */
			Console.WriteLine("Check correctness of the sequential and random bulk updates ({0}).", DateTime.Now);
			callableSql.Backend.DatabaseConnect();
			timeIt("mu_checkmod_100_seq()"	, "mu_checkmod_100_seq");
			timeIt("mu_checkmod_100_rand()"	, "mu_checkmod_100_rand");

			timeIt("mu_drop_sel100_seq()"	, "mu_drop_sel100_seq");
			timeIt("mu_drop_sel100_rand()"	, "mu_drop_sel100_rand");
			callableSql.Backend.DatabaseDisconnect();

			/* Step 8 - Run OLTP test for 15 minutes.	*/
			Console.WriteLine("Run OLTP test for 15 minutes ({0}).", DateTime.Now);
			timeToRun = 15;
			for (int i = 0; i < nInstances; i++) 
			{
				process[i] = new Thread(new ThreadStart(oltp_update));
				process[i].Name = "User " + i.ToString();
				process[i].Start();
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				process[i].Join();
			}

			/* Step 9 -- Measure throughput in IR test for five minutes.	*/
			Console.WriteLine("Run Measure throughput in IR test for five minutes ({0}).", DateTime.Now);
			iters		= 0;
			startTime	= DateTime.Now;
			timeToRun	= 5;
			for (int i = 0; i < nInstances; i++) 
			{
				process[i] = new Thread(new ThreadStart(ir_select));
				process[i].Name = "User " + i.ToString();
				process[i].Start();
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				process[i].Join();
			}

			fTime = ((double)(DateTime.Now.Ticks - startTime.Ticks)) / ticksPerSecond;
			log.Simple("Mixed OLTP (tup/sec)\t{0}"			+
						"\treturned in {1} minutes\n"		,
						Math.Round((double)iters/fTime, 4)	, 
						Math.Round(fTime/60, 4));

			/* Step 10 -- Replace one background OLTP script with the cross 
			 * section script. This is the Mixed Workload OLTP test (Mix 4). 
			 * This step is variable length.
			 */
			Console.WriteLine("Run Mixed Workload OLTP test (Mix 4) ({0}).", DateTime.Now);
			currentTest		= "Mixed OLTP";
			process[0]		= new Thread(new ThreadStart(crossSectionTests));
			process[0].Name = "User " + 0.ToString();
			process[0].Start();
			/* Exec the only one time in each thread	*/
			timeToRun	= -1;
			for (int i = 1; i < nInstances; i++)
			{
				process[i] = new Thread(new ThreadStart(oltp_update));
				process[i].Name = "User " + i.ToString();
				process[i].Start();
			}			

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				process[i].Join();
			}

			/* Step 11 -- Perform correctness checks, checkmod_100_seq and 
			 * checkmod_100_rand. Remove temporary tables: sel100seq and 
			 * sel100rand.
			 */
			Console.WriteLine("Check correctness of the sequential and random bulk updates ({0}).", DateTime.Now);
			callableSql.Backend.DatabaseConnect();
			timeIt("mu_checkmod_100_seq()"	, "mu_checkmod_100_seq");			
			timeIt("mu_checkmod_100_rand()"	, "mu_checkmod_100_rand");
			
			timeIt("mu_drop_sel100_seq()"	, "mu_drop_sel100_seq");
			timeIt("mu_drop_sel100_rand()"	, "mu_drop_sel100_rand");
			callableSql.Backend.DatabaseDisconnect();
		}


		private void crossSectionTests() 
		{
			long	startTime;
			string	currentTest = this.currentTest;

			callableSql.Backend.DatabaseConnect();

			startTime = DateTime.Now.Ticks;

			timeIt("o_mode_tiny()"		, "o_mode_tiny"			, IsolationLevel.RepeatableRead);
			timeIt("o_mode_100k()"		, "o_mode_100k"			, IsolationLevel.RepeatableRead);
			timeIt("sel_1_ncl()"		, "sel_1_ncl"			, IsolationLevel.ReadUncommitted);
			timeIt("sel_1_ncl()"		, "sel_1_ncl"			, IsolationLevel.ReadCommitted);
			timeIt("sel_1_ncl()"		, "sel_1_ncl"			, IsolationLevel.RepeatableRead);
			timeIt("agg_simple_report()", "agg_simple_report");
			timeIt("mu_sel_100_seq()"	, "mu_sel_100_seq"		, IsolationLevel.RepeatableRead);
			timeIt("mu_sel_100_rand()"	, "mu_sel_100_rand"		, IsolationLevel.RepeatableRead);
			timeIt("mu_mod_100_seq()"	, "mu_mod_100_seq"		, IsolationLevel.RepeatableRead);
			timeIt("mu_mod_100_rand()"	, "mu_mod_100_rand"		, IsolationLevel.RepeatableRead);
			timeIt("mu_unmod_100_seq()"	, "mu_unmod_100_seq"	, IsolationLevel.RepeatableRead);
			timeIt("mu_unmod_100_rand()", "mu_unmod_100_rand"	, IsolationLevel.RepeatableRead);

			callableSql.Backend.DatabaseDisconnect();

			startTime = DateTime.Now.Ticks - startTime;

			log.Simple("crossSectionTests({0})\t{1}", currentTest, 
						Math.Round((double)startTime / ticksPerSecond, 4));
		}


		private void ir_select()
		{
			DateTime	endTime		= DateTime.Now;
			CallableSql callUser	= new CallableSql(backendName);

			callUser.TupleCount = callableSql.TupleCount;

			callUser.Backend.DatabaseConnect();			

			if (timeToRun > 0)
			{
				endTime = DateTime.Now.AddMinutes(timeToRun);
				while (endTime >= DateTime.Now)
				{					
					callUser.mu_ir_select();
					iters++;
				}
			}
			else
			{
				callUser.mu_ir_select();
			}

			callUser.Backend.DatabaseDisconnect();
		}

		
		private void oltp_update()
		{
			DateTime	endTime		= DateTime.Now;
			CallableSql callUser	= new CallableSql(backendName);

			lock (callUser.Backend)
			{
				callUser.TupleCount = callableSql.TupleCount;

				callUser.Backend.DatabaseConnect();
				
				if (timeToRun > 0)
				{
					endTime = DateTime.Now.AddMinutes(timeToRun);
					while (endTime >= DateTime.Now)
					{
						callUser.mu_oltp_update();
					}
				}
				else
				{
					callUser.mu_oltp_update();
				}

				callUser.Backend.DatabaseDisconnect();
			}			
		}


		private void timeIt(string rtnName, string methodName, params object[] parameters) 
		{
			MethodInfo	method		= null;
			MethodInfo	thisMethod	= null;
			long		clocks;
			int			retval;

			currentTest				= rtnName;
			callableSql.TestFailed	= false;

			method = callableSql.GetType().GetMethod(methodName);			
			if (method == null)
			{
				thisMethod = this.GetType().GetMethod(methodName, BindingFlags.NonPublic|BindingFlags.Instance|BindingFlags.DeclaredOnly);
			}
			clocks = DateTime.Now.Ticks;

			if (method != null)
			{
				retval = (int)method.Invoke(callableSql, parameters);
			}
			else
			{
				retval = (int)thisMethod.Invoke(this, parameters);
			}

			clocks		= DateTime.Now.Ticks - clocks;

			int length = 40 - rtnName.Length;
			for (int i = 0; i < length; i++)
			{
				rtnName = " " + rtnName;
			}

			if (callableSql.TestFailed)
			{
				log.Simple("{0}\tfailed\t{0}\t{1}\t{2}"					,
							rtnName										,
							Math.Round((double)clocks/ticksPerSecond)	,
							retval);
			}
			else
			{
				log.Simple("{0}\t{1} seconds\treturn value = {2} \t\t with {3}",
							rtnName										,
							Math.Round((double)clocks/ticksPerSecond, 4),
							retval										,
					parameters.Length > 0 ? parameters[0] : "nothing");
			}
		}

		#endregion
	}
}
