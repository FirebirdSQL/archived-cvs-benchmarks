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
using System.Data;
using System.Text;
using System.Threading;
using System.Reflection;
using System.Configuration;

using CSharp.Logger;

namespace AS3AP.BenchMark
{
	public class TestResultEventArgs : EventArgs
	{
		#region FIELDS

		private string		testName	= String.Empty;
		private object		testResult;
		private TimeSpan	testTime;
		private bool		testFailed	= false;

		#endregion

		#region PROPERTIES

		public string TestName
		{
			get { return testName; }
			set { testName = value; }
		}

		public object TestResult
		{
			get { return testResult; }
			set { testResult = value; }
		}

		public TimeSpan TestTime
		{
			get { return testTime; }
			set { testTime = value; }
		}

		public bool TestFailed
		{
			get { return testFailed; }
			set { testFailed = value; }
		}

		#endregion

		#region CONSTRUCTORS

		public TestResultEventArgs(string testName, object testResult, TimeSpan testTime, bool testFailed)
		{
			this.testName	= testName;
			this.testResult = testResult;
			this.testTime	= testTime;
			this.testFailed	= testFailed;
		}

		#endregion
	}

	public class ProgressMessageEventArgs : EventArgs
	{
		#region FIELDS

		private string	message	= String.Empty;

		#endregion

		#region PROPERTIES

		public string Message
		{
			get { return message; }
			set { message = value; }
		}

		#endregion

		#region CONSTRUCTORS

		public ProgressMessageEventArgs(string message)
		{
			this.message = message;
		}

		#endregion
	}

	#region DELEGATES

	public delegate void TestResultEventHandler(object sender, TestResultEventArgs e);
	public delegate void ProgressMessageEventHandler(object sender, ProgressMessageEventArgs e);

	#endregion

	public class AS3AP : IDisposable
	{	
		#region EVENTS

		public event TestResultEventHandler			TestResult;
		public event ProgressMessageEventHandler	ProgressMessage;

		#endregion

		#region FIELDS

		private BenchMarkConfiguration	configuration;

		private bool		disposed		= false;

		private	Logger		log;
		private	long		ticksPerSecond	= TimeSpan.TicksPerSecond;		
		private ITestSuite	testSuite;
		private string		currentTest		= String.Empty;
		private int			iters			= 0;
		private int			timeToRun		= 15;
		private int			tupleCount		= 0;
		private string		testSuiteType	= "SQL87";

		#endregion

		#region CONSTRUCTORS

		public AS3AP(BenchMarkConfiguration	configuration)
		{
			string logName = "as3ap_"								+
							System.DateTime.Now.Year.ToString()		+
							System.DateTime.Now.Month.ToString()	+
							System.DateTime.Now.Day.ToString()		+
							System.DateTime.Now.Hour.ToString()		+
							System.DateTime.Now.Minute.ToString()	+
							System.DateTime.Now.Second.ToString()	+
							".log";
			
			if (configuration.EnableLogging)
			{
				this.log = new Logger(logName, Mode.OVERWRITE);		
			}
			this.configuration	= configuration;
	
			testSuite		= TestSuiteFactory.GetTestSuite(testSuiteType, configuration);
		}

		#endregion

		#region IDISPOSABLE_METHODS

		~AS3AP()
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
						if (log != null)
						{
							log.Close();
							log = null;
						}
						testSuite.Dispose();
						testSuite = null;
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

		#region METHODS

		public void Run()
		{
			TimeSpan	elapsed;
			long		clocks			= 0;
			int			dbSize			= 0;
			int			singleUserCount = 0;
			int			multiUserCount	= 0;
			
			if (log != null) log.Simple("Starting as3ap benchmark at: {0}", DateTime.Now);

			if (configuration.RunCreate) 
			{
				if (ProgressMessage != null)
				{
					ProgressMessage(this, 
						new ProgressMessageEventArgs("Creating tables and loading data " + DateTime.Now.ToString()));
				}
				runTest("runCreateDataBase");
			}			
			else
			{
				runDatabaseSetup();
			}

			testSuite.Backend.DatabaseConnect();
			if ((tupleCount = testSuite.CountRows("updates")) == 0)
			{
				if (log != null) log.Simple("empty database -- empty results");
				return;
			}		
			testSuite.TupleCount = tupleCount;
			dbSize = (4 * testSuite.TupleCount * 100)/1000000;
			
			if (log != null) log.Simple("\r\n\"Database size {0}MB\"\r\n", dbSize);

			testSuite.Backend.DatabaseDisconnect();			

			string[] testSequence = configuration.RunSequence.Split(';');

			for (int i = 0; i < testSequence.Length; i++)
			{
				string[] testType = testSequence[i].Split(':');

				for (int j = 0; j < testType.Length; j++)				
				{
					switch (testType[j].ToUpper())
					{
						case "SQL87":
						case "SQL92":
						{
							if (ProgressMessage != null)
							{
								ProgressMessage(this, 
									new ProgressMessageEventArgs("Running tests using " + testType[j] + " syntax"));
							}
						
							testSuite.Backend.CloseLogger();
							testSuite = TestSuiteFactory.GetTestSuite(testSuiteType, configuration);
							testSuite.TupleCount = tupleCount;

							if (log != null) log.Simple("\r\n\"Running tests using {0} syntax\r\n\"", testType[j]);
						}
						break;

						case "SINGLEUSER":
						{
							/* Start of the single user test */
							if (ProgressMessage != null)
							{
								ProgressMessage(this, 
									new ProgressMessageEventArgs("Preparing single-user test"));
							}
							if (singleUserCount != 0)
							{
								runDatabaseSetup();
							}

							if (ProgressMessage != null)
							{
								ProgressMessage(this, 
									new ProgressMessageEventArgs("Starting single-user test"));
							}

							if (log != null) log.Simple("\r\n");

							testSuite.Backend.DatabaseDisconnect();
							if (ProgressMessage != null)
							{
								ProgressMessage(this, 
									new ProgressMessageEventArgs("Starting multi-user test"));
							}
												
							clocks = DateTime.Now.Ticks;
							runSingleUserTests();
							elapsed = new TimeSpan(DateTime.Now.Ticks - clocks);

							if (log != null) log.Simple("\r\nSingle user test ( {0} )\r\n\r\n",
												 elapsed.ToString());


							singleUserCount++;
						}
						break;

						case "MULTIUSER":
						{
							/* Start of the multi-user test */
							currentTest = "Preparing multi-user test";
							testSuite.Backend.DatabaseConnect();
							if (testSuite.TupleCount != testSuite.CountRows("updates")) 
							{
								if (log != null) log.Simple("Invalid data ( skipping multi-user test )");
							}
							else
							{
								testSuite.Backend.DatabaseDisconnect();
								if (ProgressMessage != null)
								{
									ProgressMessage(this, 
										new ProgressMessageEventArgs("Starting multi-user test"));
								}
												
								clocks = DateTime.Now.Ticks;
								runMultiUserTests(configuration.UserNumber == 0 ? (int)(dbSize / 4) : configuration.UserNumber);
								elapsed = new TimeSpan(DateTime.Now.Ticks - clocks);

								if (log != null) log.Simple("\r\nMulti user test ( {0} )\r\n\r\n",
											elapsed.ToString());

								multiUserCount++;
							}
						}
						break;
					}
				}
			}

			if (ProgressMessage != null)
			{
				ProgressMessage(this, 
					new ProgressMessageEventArgs("!!! Finished !!!"));
			}
		}

		private int runCreateDataBase() 
		{
			testSuite.Backend.DatabaseCreate("AS3AP");

			testSuite.Backend.DatabaseConnect();
			runTest("create_tables");
			runTest("LoadData");
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
			testSuite.Backend.DatabaseDisconnect();
			
			return 0;
		}

		private void runDatabaseSetup()
		{
			testSuite.Backend.DatabaseConnect();
			testSuite.setup_database();
			testSuite.Backend.DatabaseDisconnect();
		}

		private void runSingleUserTests() 
		{
			testSuite.Backend.DatabaseConnect();
			
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

			testSuite.Backend.DatabaseDisconnect();
		}

		private void runMultiUserTests(int nInstances) 
		{	
			double		fTime;
			DateTime	startTime;
			Thread[]	process = new Thread[nInstances];
			
			if (log != null) log.Simple("\"Executing multi-user tests with {0} user task{1}\"\n",
						nInstances, ((nInstances != 1) ? "s" : ""));

			/* Step 1 -- Backup updates relation, including indices, 
			 * to tape or other device. This is done early on.
			 */
			
			/* Step 2 -- Run IR (Mix 1) test for 15 minutes.	*/
			if (ProgressMessage != null)
			{
				ProgressMessage(this, 
					new ProgressMessageEventArgs("Run IR (Mix 1) test for 15 minutes (" + DateTime.Now.ToString() + ")."));
			}
			iters		= 0;
			timeToRun	= 15;
			for (int i = 0; i < nInstances; i++) 
			{
				process[i]		= new Thread(new ThreadStart(runIrSelect));
				process[i].Name = "User " + i.ToString();
				process[i].Start();
			}
			
			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				process[i].Join();
			}
						
			/* Step 3 -- Measure throughput in IR test for five minutes.	*/
			if (ProgressMessage != null)
			{
				ProgressMessage(this, 
					new ProgressMessageEventArgs("Run Measure throughput in IR test for five minutes (" + DateTime.Now.ToString() + ")."));
			}
			iters		= 0;
			startTime	= DateTime.Now;
			timeToRun	= 5;
			for (int i = 0; i < nInstances; i++) 
			{
				process[i] = new Thread(new ThreadStart(runIrSelect));
				process[i].Name = "User " + i.ToString();
				process[i].Start();
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				process[i].Join();
			}

			fTime = ((double)(DateTime.Now.Ticks - startTime.Ticks)) / ticksPerSecond;
			if (log != null) log.Simple("Mixed IR (tup/sec)\t{0}"			+
						"\treturned in {1} minutes"			,
						Math.Round((double)iters/fTime, 4)	, 
						Math.Round(fTime/60, 4));

			/* Step 4 -- A Mixed Workload IR Test, where one user executes a cross 
			 * section of ten update and retrieval queries, and all the others 
			 * execute the same IR query as in the second test.
			 */
			if (ProgressMessage != null)
			{
				ProgressMessage(this, 
					new ProgressMessageEventArgs("Run Mixed Workload IR test (Mix 3) (" + DateTime.Now.ToString() + ")."));
			}
			currentTest		= "Mixed IR";
			process[0]		= new Thread(new ThreadStart(runCrossSectionTests));
			process[0].Name = "User " + 0.ToString();
			process[0].Start();
			/* Exec the only one time in each thread	*/
			timeToRun	= -1;
			for (int i = 1; i < nInstances; i++) 
			{
				process[i] = new Thread(new ThreadStart(runIrSelect));
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
			if (ProgressMessage != null)
			{
				ProgressMessage(this, 
					new ProgressMessageEventArgs("Check correctness of the sequential and random bulk updates (" + DateTime.Now.ToString() + ")."));
			}
			testSuite.Backend.DatabaseConnect();
			runTest("mu_checkmod_100_seq");
			runTest("mu_checkmod_100_rand");
			testSuite.Backend.DatabaseDisconnect();

			/* Step 6 - Recover updates relation from backup tape (Step 1) 
			 * and log (from Steps 2, 3, 4, and 5).	
			 */


			/* Step 7 - Perform correctness checks, checkmod_100_seq and 
			 * checkmod_100_rand. Remove temporary tables: sel100seq and 
			 * sel100rand.
			 */
			if (ProgressMessage != null)
			{
				ProgressMessage(this, 
					new ProgressMessageEventArgs("Check correctness of the sequential and random bulk updates (" + DateTime.Now.ToString() + ")."));
			}
			testSuite.Backend.DatabaseConnect();
			runTest("mu_checkmod_100_seq");
			runTest("mu_checkmod_100_rand");

			runTest("mu_drop_sel100_seq");
			runTest("mu_drop_sel100_rand");
			testSuite.Backend.DatabaseDisconnect();

			/* Step 8 - Run OLTP test for 15 minutes.	*/
			if (ProgressMessage != null)
			{
				ProgressMessage(this, 
					new ProgressMessageEventArgs("Run OLTP test for 15 minutes (" + DateTime.Now.ToString() + ")."));
			}
			timeToRun = 15;
			for (int i = 0; i < nInstances; i++) 
			{
				process[i] = new Thread(new ThreadStart(runOltpUpdate));
				process[i].Name = "User " + i.ToString();
				process[i].Start();
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				process[i].Join();
			}

			/* Step 9 -- Measure throughput in IR test for five minutes.	*/
			if (ProgressMessage != null)
			{
				ProgressMessage(this, 
					new ProgressMessageEventArgs("Run Measure throughput in IR test for five minutes (" + DateTime.Now.ToString() + ")."));
			}
			iters		= 0;
			startTime	= DateTime.Now;
			timeToRun	= 5;
			for (int i = 0; i < nInstances; i++) 
			{
				process[i] = new Thread(new ThreadStart(runIrSelect));
				process[i].Name = "User " + i.ToString();
				process[i].Start();
			}

			/* Wait to the end of the threads	*/
			for (int i = 0; i < nInstances; i++) 
			{
				process[i].Join();
			}

			fTime = ((double)(DateTime.Now.Ticks - startTime.Ticks)) / ticksPerSecond;
			if (log != null) log.Simple("Mixed OLTP (tup/sec)\t{0}"			+
						"\treturned in {1} minutes\n"		,
						Math.Round((double)iters/fTime, 4)	, 
						Math.Round(fTime/60, 4));

			/* Step 10 -- Replace one background OLTP script with the cross 
			 * section script. This is the Mixed Workload OLTP test (Mix 4). 
			 * This step is variable length.
			 */
			if (ProgressMessage != null)
			{
				ProgressMessage(this, 
					new ProgressMessageEventArgs("Run Mixed Workload OLTP test (Mix 4) (" + DateTime.Now.ToString() + ")."));
			}
			currentTest		= "Mixed OLTP";
			process[0]		= new Thread(new ThreadStart(runCrossSectionTests));
			process[0].Name = "User " + 0.ToString();
			process[0].Start();
			/* Exec the only one time in each thread	*/
			timeToRun	= -1;
			for (int i = 1; i < nInstances; i++)
			{
				process[i] = new Thread(new ThreadStart(runOltpUpdate));
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
			if (ProgressMessage != null)
			{
				ProgressMessage(this, 
					new ProgressMessageEventArgs("Check correctness of the sequential and random bulk updates (" + DateTime.Now.ToString() + ")."));
			}
			testSuite.Backend.DatabaseConnect();
			runTest("mu_checkmod_100_seq");			
			runTest("mu_checkmod_100_rand");
			
			runTest("mu_drop_sel100_seq");
			runTest("mu_drop_sel100_rand");
			testSuite.Backend.DatabaseDisconnect();
		}

		private void runCrossSectionTests() 
		{
			long	startTime;
			long	endTime;

			testSuite.Backend.DatabaseConnect();

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

			testSuite.Backend.DatabaseDisconnect();

			if (log != null) log.Simple("CrossSectionTests({0})\t{1}", 
										currentTest, 
										elapsed.ToString());
		}

		private void runIrSelect()
		{
			DateTime	endTime	= DateTime.Now;
			ITestSuite	test	= TestSuiteFactory.GetTestSuite(testSuiteType, configuration);

			test.TupleCount = testSuite.TupleCount;

			test.Backend.DatabaseConnect();			

			if (timeToRun > 0)
			{
				endTime = DateTime.Now.AddMinutes(timeToRun);
				while (endTime >= DateTime.Now)
				{					
					test.mu_ir_select();
					iters++;
				}
			}
			else
			{
				test.mu_ir_select();
			}

			test.Backend.DatabaseDisconnect();
		}
		
		private void runOltpUpdate()
		{
			DateTime	endTime	= DateTime.Now;
			ITestSuite	test	= TestSuiteFactory.GetTestSuite(testSuiteType, configuration);

			lock (test.Backend)
			{
				test.TupleCount = testSuite.TupleCount;

				test.Backend.DatabaseConnect();
				
				if (timeToRun > 0)
				{
					endTime = DateTime.Now.AddMinutes(timeToRun);
					while (endTime >= DateTime.Now)
					{
						test.mu_oltp_update();
					}
				}
				else
				{
					test.mu_oltp_update();
				}

				test.Backend.DatabaseDisconnect();
			}			
		}

		private void runTest(string testName)
		{
			MethodInfo	method		= null;
			MethodInfo	thisMethod	= null;
			long		clocks;

			currentTest				= testName;
			testSuite.TestFailed	= false;

			method = testSuite.GetType().GetMethod(testName);			
			if (method == null)
			{
				thisMethod = this.GetType().GetMethod(testName, BindingFlags.NonPublic|BindingFlags.Instance|BindingFlags.DeclaredOnly);
			}
			
			// Set IsolationLevel for test execution
			testSuite.SetIsolationLevel(testName);

			// Reset TestFailed property value
			testSuite.TestFailed = false;

			clocks = DateTime.Now.Ticks;

			if (method != null)
			{
				method.Invoke(testSuite, null);
			}
			else
			{
				thisMethod.Invoke(this, null);
			}

			clocks	= DateTime.Now.Ticks - clocks;

			testName = formatMethodName(testName);

			TimeSpan elapsed = new TimeSpan(clocks);				

			if (TestResult != null)
			{
				TestResult(this, new TestResultEventArgs(testName, testSuite.TestResult, elapsed, testSuite.TestFailed));
			}

			StringBuilder logMessage = new StringBuilder();

			if (testSuite.TestFailed)
			{
				if (log != null) log.Simple("-----> {0}\tfailed <-----", testName);
			}
			else
			{
				logMessage.AppendFormat(
							"{0} ( {1} )\treturn value = {2} \t\t"	,
							testName								,
							elapsed.ToString(),
							testSuite.TestResult);
			}

			if (log != null) log.Simple(logMessage.ToString());
		}

		private string formatMethodName(string methodName)
		{
			int length = 30 - methodName.Length;

			for (int i = 0; i < length; i++)
			{
				methodName = " " + methodName;
			}

			return methodName;
		}

		#endregion
	}
}