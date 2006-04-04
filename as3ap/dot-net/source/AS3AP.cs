//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
// Copyright (C) 2002-2004  Carlos Guzman Alvarez
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
using System.IO;
using System.Data;
using System.Text;
using System.Threading;
using System.Reflection;
using System.Configuration;

namespace AS3AP.BenchMark
{
	public class AS3AP : IDisposable
	{	
		#region · Events ·

		public event TestResultEventHandler			TestResult;
		public event ProgressMessageEventHandler	ProgressMessage;

		#endregion

		#region · Fields ·

		private BenchMarkConfiguration	configuration;
		private bool					disposed		= false;
		private ITestSuite				testSuite;
		private string					currentTest		= String.Empty;
		private int						tupleCount		= 0;
		private string					testSuiteType	= "SQL87";
		private string					logPath;
		private Logger					log;
		
		#endregion

		#region · Constructors ·

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

		#region · Finalizer ·

		~AS3AP()
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
						// Close logger
						this.log.Close();
						this.log = null;

						// release any managed resources
						this.testSuite.Dispose();
						this.testSuite = null;
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

		#region · Methods ·

		public void Run()
		{
			try
			{
				if (this.testSuite.Log != null)
				{
					testSuite.Log.Simple("Starting as3ap benchmark at: {0} \r\n\r\n", DateTime.Now);
				}

				string[] testSequence = this.configuration.RunSequence.Split(';');

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
								if (this.ProgressMessage != null)
								{
									this.ProgressMessage(
										this, 
										new ProgressMessageEventArgs("Running tests using " + testType[j] + " syntax"));
								}
						
								if (this.TestResult != null)
								{
									this.TestResult(
										this, 
										new TestResultEventArgs("Running using [" + testType[j] + "] syntax", 
										0, 
										new TimeSpan(0), 
										false));									
								}

								this.testSuite 		= TestSuiteFactory.GetTestSuite(testSuiteType, configuration);
								this.testSuite.Log	= log;
								this.testSuite.TupleCount = tupleCount;

								this.testSuite.Result	+= new ResultEventHandler(OnResult);
								this.testSuite.Progress	+= new ProgressEventHandler(OnProgress);

								if (this.testSuite.Log != null)
								{
									this.testSuite.Log.Simple("\r\n\"Running tests using {0} syntax\"\r\n", testType[j]);
								}
							}
							break;

							case "SINGLEUSER":
							{
								if (this.configuration.RunCreate)
								{
									this.createDatabase();
								}
								else
								{
									this.testSuite.ConnectDatabase();
									this.testSuite.TupleCount = this.testSuite.CountRows("updates");
									this.testSuite.DisconnectDatabase();
								}

								if (this.ProgressMessage != null)
								{
									this.ProgressMessage(
										this, 
										new ProgressMessageEventArgs("Starting single-user test"));
								}

								if (this.testSuite.Log != null)
								{
									this.testSuite.Log.Simple("\r\n");
								}
																			
								this.testSuite.SingleUserTests();
							}
							break;

							case "MULTIUSER":
							{
								if (this.configuration.RunCreate)
								{
									this.createDatabase();
								}
								else
								{
									this.testSuite.ConnectDatabase();
									this.testSuite.TupleCount = this.testSuite.CountRows("updates");
									this.testSuite.DisconnectDatabase();
								}

								/* Start of the multi-user test */
								this.currentTest = "Preparing multi-user test";
								this.testSuite.ConnectDatabase();
								if (this.testSuite.TupleCount != this.testSuite.CountRows("updates")) 
								{
									this.testSuite.DisconnectDatabase();															
									if (this.testSuite.Log != null)
									{
										this.testSuite.Log.Simple("Invalid data ( Multi user tests )");
									}
									throw new InvalidOperationException("Invalid data ( Multi user tests ).");
								}
								else
								{
									this.testSuite.DisconnectDatabase();
									if (this.ProgressMessage != null)
									{
										this.ProgressMessage(
											this, 
											new ProgressMessageEventArgs("Starting multi-user test"));
									}

									int dbSize = dbSize = (4 * this.testSuite.TupleCount * 100)/1000000;;
																
									this.testSuite.MultiUserTests(configuration.UserNumber == 0 ? (int)(dbSize / 4) : configuration.UserNumber);
								}
							}
							break;
						}
					}
				}

				if (this.ProgressMessage != null)
				{
					this.ProgressMessage(
						this, 
						new ProgressMessageEventArgs("!!! Finished !!!"));
				}
			}
			catch (Exception ex)
			{
				this.testSuite.DisconnectDatabase();
				this.testSuite.Dispose();

				throw ex;
			}
		}

		private void createDatabase()
		{
			int	dbSize = 0;

			if (this.ProgressMessage != null)
			{
				this.ProgressMessage(
					this, 
					new ProgressMessageEventArgs("Creating tables and loading data " + DateTime.Now.ToString()));
			}

			this.testSuite.CreateDatabase();

			this.testSuite.ConnectDatabase();
			if ((tupleCount = this.testSuite.CountRows("updates")) == 0)
			{
				if (this.testSuite.Log != null)
				{
					testSuite.Log.Simple("Database tables are empty. ( ERROR )");
				}
				throw new InvalidOperationException("Database tables are empty.");
			}		
			this.testSuite.TupleCount = tupleCount;
			dbSize = (4 * this.testSuite.TupleCount * 100)/1000000;
			
			if (this.testSuite.Log != null)
			{
				testSuite.Log.Simple("\r\n\"Database size {0}MB\"\r\n", dbSize);
			}

			this.testSuite.DisconnectDatabase();
		}

		private void OnResult(object Sender, TestResultEventArgs e)
		{
			if (this.TestResult != null)
			{
				this.TestResult(this, e);
			}
		}

		private void OnProgress(object Sender, ProgressMessageEventArgs e)
		{
			if (this.ProgressMessage != null)
			{
				this.ProgressMessage(this, e);
			}
		}

		#endregion
	}
}
