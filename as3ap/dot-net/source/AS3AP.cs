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
using System.Text;
using System.Threading;
using System.Reflection;
using System.Configuration;

using CSharp.Logger;

namespace AS3AP.BenchMark
{
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
		private ITestSuite				testSuite;
		private Logger					log;
		private string					logPath;
		private bool					disposed		= false;
		private string					currentTest		= String.Empty;
		private string					testSuiteType	= "SQL87";
		private int						tupleCount		= 0;
				
		#endregion

		#region CONSTRUCTORS

		public AS3AP(string logPath, BenchMarkConfiguration	configuration)
		{
			this.configuration	= configuration;

			this.logPath = logPath;
			if (!this.logPath.EndsWith(Path.DirectorySeparatorChar.ToString()))
			{
				this.logPath += Path.DirectorySeparatorChar;
			}

			string logName = this.logPath + "as3ap_"	+
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
	
			testSuite		= TestSuiteFactory.GetTestSuite(testSuiteType, configuration);			
			testSuite.Log	= log;

			testSuite.Result	+= new ResultEventHandler(OnResult);
			testSuite.Progress	+= new ProgressEventHandler(OnProgress);
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
						// Close logger
						log.Close();
						log = null;

						// release any managed resources
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
			try
			{
				int	dbSize			= 0;
				int	singleUserCount = 0;
				int	multiUserCount	= 0;
			
				if (testSuite.Log != null) testSuite.Log.Simple("Starting as3ap benchmark at: {0} \r\n\r\n", DateTime.Now);

				if (configuration.RunCreate) 
				{
					if (ProgressMessage != null)
					{
						ProgressMessage(this, 
							new ProgressMessageEventArgs("Creating tables and loading data " + DateTime.Now.ToString()));
					}
					testSuite.create_database();
				}			
				else
				{
					testSuite.setup_database();
				}

				testSuite.DatabaseConnect();
				if ((tupleCount = testSuite.count_rows("updates")) == 0)
				{
					if (testSuite.Log != null) testSuite.Log.Simple("Database tables are empty. ( ERROR )");
					throw new InvalidOperationException("Database tables are empty.");
				}		
				testSuite.TupleCount = tupleCount;
				dbSize = (4 * testSuite.TupleCount * 100)/1000000;
			
				if (testSuite.Log != null) testSuite.Log.Simple("\r\n\"Database size {0}MB\"\r\n", dbSize);

				testSuite.DatabaseDisconnect();			

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
						
								if (TestResult != null)
								{
									TestResult(this, 
										new TestResultEventArgs("Running using [" + testType[j] + "] syntax", 
										0, 
										new TimeSpan(0), 
										false));									
								}

								testSuite 		= TestSuiteFactory.GetTestSuite(testSuiteType, configuration);
								testSuite.Log	= log;
								testSuite.TupleCount = tupleCount;

								testSuite.Result	+= new ResultEventHandler(OnResult);
								testSuite.Progress	+= new ProgressEventHandler(OnProgress);

								if (testSuite.Log != null) testSuite.Log.Simple("\r\n\"Running tests using {0} syntax\"\r\n", testType[j]);
							}
								break;

							case "SINGLEUSER":
							{						
								if (singleUserCount != 0)
								{
									/* Start of the single user test */
									if (ProgressMessage != null)
									{
										ProgressMessage(this, 
											new ProgressMessageEventArgs("Preparing single-user test"));
									}

									testSuite.setup_database();
								}

								if (ProgressMessage != null)
								{
									ProgressMessage(this, 
										new ProgressMessageEventArgs("Starting single-user test"));
								}

								if (testSuite.Log != null) testSuite.Log.Simple("\r\n");
																			
								testSuite.single_user_tests();

								singleUserCount++;
							}
								break;

							case "MULTIUSER":
							{
								/* Start of the multi-user test */
								currentTest = "Preparing multi-user test";
								testSuite.DatabaseConnect();
								if (testSuite.TupleCount != testSuite.count_rows("updates")) 
								{
									testSuite.DatabaseDisconnect();															
									if (testSuite.Log != null) testSuite.Log.Simple("Invalid data ( Multi user tests )");
									throw new InvalidOperationException("Invalid data ( Multi user tests ).");
								}
								else
								{
									testSuite.DatabaseDisconnect();
									if (ProgressMessage != null)
									{
										ProgressMessage(this, 
											new ProgressMessageEventArgs("Starting multi-user test"));
									}
																				
									testSuite.multi_user_tests(configuration.UserNumber == 0 ? (int)(dbSize / 4) : configuration.UserNumber);

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
			catch (Exception ex)
			{
				testSuite.DatabaseDisconnect();
				testSuite.Dispose();

				throw ex;
			}
		}

		private void OnResult(object Sender, TestResultEventArgs e)
		{
			if (TestResult != null)
			{
				TestResult(this, e);
			}
		}

		private void OnProgress(object Sender, ProgressMessageEventArgs e)
		{
			if (ProgressMessage != null)
			{
				ProgressMessage(this, e);
			}
		}

		#endregion
	}
}
