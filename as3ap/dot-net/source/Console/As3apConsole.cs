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
using System.Configuration;
using System.IO;
using System.Text;
using System.Threading;

namespace AS3AP.BenchMark
{
	internal class As3apConsole
	{
		#region Fields

		private BenchMarkConfiguration		configuration = new BenchMarkConfiguration();
		private Thread						runThread;
		private bool						isRunning = false;
		private AS3AP						as3ap;
		private TestResultEventHandler		testResultHandler;
		private ProgressMessageEventHandler	progressMessageHandler;

		#endregion

		#region Constructors

		private As3apConsole()
		{
		}

		#endregion

		#region Public Static Methods

		public static void Run()
		{
			As3apConsole console = new As3apConsole();

			try
			{
				// Load Default config file.
				console.loadDefaultConfig();

				// Run the benchmark
				console.run();
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.ToString());
			}
		}

		#endregion

		#region Private Methods

		private void run()
		{
			// Create a new thread
			this.runThread = new Thread(new ThreadStart(this.runBenchMark));

			// Configure Thread
			this.runThread.IsBackground = true;

			// Start Thread execution
			this.runThread.Start();

			// Wiat for the end of the benchmark execution
			this.runThread.Join();
		}

		private void runBenchMark()
		{
			try
			{
				this.as3ap = new AS3AP(
					Path.GetDirectoryName(Environment.CurrentDirectory),
					this.configuration);
			
				// Test Result Event handler
				this.testResultHandler	= new TestResultEventHandler(OnTestResult);
				this.as3ap.TestResult	+= this.testResultHandler;

				// Progress Message Event handler
				this.progressMessageHandler	= new ProgressMessageEventHandler(OnProgressMessage);
				this.as3ap.ProgressMessage	+= this.progressMessageHandler;

				this.isRunning = true;

				this.as3ap.Run();
			}
			catch (ThreadAbortException)
			{
			}
			catch (Exception ex)
			{
				StringBuilder e = new StringBuilder();

				e.AppendFormat("Message: \r\n {0} \r\n StackTrace: \r\n {1}", ex.Message, ex.StackTrace);
				
				Console.WriteLine(ex.ToString());
			}
			finally
			{
				if (this.testResultHandler != null)
				{
					this.as3ap.TestResult -= testResultHandler;
				}
				if (this.progressMessageHandler != null)
				{
					this.as3ap.ProgressMessage -= progressMessageHandler;
				}
				this.as3ap.Dispose();
				this.as3ap		= null;
				this.isRunning	= false;
			}
		}

		private void loadDefaultConfig()
		{
			string defaultConfig = ConfigurationSettings.AppSettings["DefaultConfigFile"];

			if (defaultConfig != null)
			{
				if (File.Exists(defaultConfig))
				{
					FileInfo info = new FileInfo(defaultConfig);

					this.configuration = BenchMarkConfiguration.Load(
						info.FullName);
				}
			}
		}

		private void OnTestResult(object Sender, TestResultEventArgs e)
		{
			string line = String.Format(
				"{0} [{1}] [{2}] [{3}] [4]",
				e.TestName.Trim(),
				e.TestResult.ToString(),
				e.TestTime.ToString(),
				e.TestFailed.ToString());

			Console.WriteLine(line);
		}

		private void OnProgressMessage(object Sender, ProgressMessageEventArgs e)
		{
			Console.WriteLine("\t\t\t\t{0}", e.Message.ToString());
		}

		#endregion
	}
}
