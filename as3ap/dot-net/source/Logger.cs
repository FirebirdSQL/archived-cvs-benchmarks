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
using System.Diagnostics;
using System.IO;
using System.Text;

namespace AS3AP.BenchMark
{
	#region Enumerations

	/// <summary>
	/// Messages priorities
	/// </summary>
	public enum Priority
	{
		DEBUG,
		WARN,		
		ERROR,
		INFO,
		FATAL
	}

	/// <summary>
	/// Log modes
	/// </summary>
	public enum Mode
	{
		OVERWRITE,
		APPEND	
	}

	#endregion

	/// <summary>
	/// Allow creation of log files for th eoperations of te code.
	/// </summary>
	public class Logger : IDisposable
	{		
		#region Fields

		/// <summary>
		/// Log file stream		
		/// </summary>		
		private Stream streamFile = null;
		
		/// <summary>
		/// Listener
		/// </summary>
		private TextWriterTraceListener debugListener = null;

		/// <summary>
		/// Debug messages logging enabled 
		/// </summary>
		private bool debugEnabled	= true;
		/// <summary>
		/// Warn messages logging enabled
		/// </summary>
		private bool warnEnabled	= true;
		/// <summary>
		/// Error messages logging enabled
		/// </summary>
		private bool errorEnabled	= true;
		/// <summary>
		/// Info messages logging enabled
		/// </summary>
		private bool infoEnabled	= true;
		/// <summary>
		/// Fatal messages logging enabled
		/// </summary>
		private bool fatalEnabled	= true;

		private bool disposed		= false;

		#endregion

		#region Properties

		/// <summary>
		/// Debug messages enabled property
		/// </summary>
		public bool DebugEnabled
		{
			get { return debugEnabled; }
			set { debugEnabled = value; }
		}

		/// <summary>
		/// Warn messages enabled property
		/// </summary>
		public bool WarnEnabled
		{
			get { return warnEnabled; }
			set { warnEnabled = value; }
		}

		/// <summary>
		/// Error messages enabled property
		/// </summary>
		public bool ErrorEnabled
		{
			get { return errorEnabled; }
			set { errorEnabled = value; }
		}

		/// <summary>
		/// Info messages enabled property
		/// </summary>
		public bool InfoEnabled
		{
			get { return infoEnabled; }
			set { infoEnabled=value; }
		}

		/// <summary>
		/// Fatal messages enabled property
		/// </summary>
		public bool FatalEnabled
		{
			get { return fatalEnabled; }
			set { fatalEnabled = value; }
		}
		
		#endregion

		#region Constructors

		/// <summary>
		/// Initializes a new instance of Log4CSharp class
		/// </summary>
		/// <param name="type">Class type</param>
		/// <param name="fileName">Path to log file</param>
		/// <param name="mode">Access mode to the log file</param>
		// public Logger(Type type, string fileName, Mode mode)
		public Logger(string fileName, Mode mode)
		{
			System.Diagnostics.Debug.Assert(fileName!=null, "Logger: You need to indicate a log file.");

			try
			{
				switch(mode)
				{
					case Mode.OVERWRITE:
						// Overwrite file
						streamFile = File.Create(fileName);
						streamFile.Close();
						// Reopen it in APPEND/SHARED mode
						streamFile = File.Open(fileName,FileMode.Append,FileAccess.Write,FileShare.ReadWrite);
						break;
				
					case Mode.APPEND:
						streamFile = File.Open(fileName,FileMode.Append,FileAccess.Write,FileShare.ReadWrite);
						break;

					default:
						streamFile = File.Create(fileName);
						break;
				}
				debugListener = new TextWriterTraceListener(streamFile);

				Trace.Listeners.Add(debugListener);
			}
			catch (IOException e)
			{
				throw e;
			}
		}

		#endregion 

		#region Finalizers

		~Logger()
		{
			this.Dispose(false);
		}

		public void Dispose()
		{
			this.Dispose(true);

			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (!disposed)
			{
				if (disposing)
				{
					Close();
				}
			}

			disposed = true;
		}

		#endregion

		#region Methods

		/// <summary>
		/// Close logger
		/// </summary>
		public void Close()
		{
			streamFile.Close();
		}

		/// <summary>
		/// Generic Log method
		/// </summary>
		/// <param name="logType">Message type</param>
		/// <param name="obj">Object</param>
		public virtual void Log(Priority logType, object obj)
		{
			switch(logType)
			{
				case Priority.DEBUG:
					Debug("{0}", obj);
					break;

				case Priority.WARN:
					Warn("{0}", obj);
					break;

				case Priority.ERROR:
					Error("{0}", obj);
					break;

				case Priority.INFO:
					Info("{0}", obj);
					break;

				case Priority.FATAL:
					Fatal("{0}", obj);
					break;

				default:
					break;
			}
		}
		
		/// <summary>
		/// Generic Log method - With exception
		/// </summary>
		/// <param name="logType">Message type</param>
		/// <param name="obj">Object</param>
		/// <param name="ex">Exception</param>
		public virtual void Log(Priority logType, object obj, Exception ex)
		{
			switch(logType)
			{
				case Priority.DEBUG:
					DebugEx(obj, ex);
					break;

				case Priority.WARN:
					WarnEx(obj, ex);
					break;

				case Priority.ERROR:
					ErrorEx(obj, ex);
					break;

				case Priority.INFO:
					InfoEx(obj, ex);
					break;

				case Priority.FATAL:
					FatalEx(obj, ex);
					break;

				default:
					break;
			}
		}

		/// <summary>
		/// Generic Log method - With format
		/// </summary>
		/// <param name="logType">Message type</param>
		/// <param name="strFormat">String format</param>
		/// <param name="args">Parameter list</param>
		public virtual void Log(Priority logType, string strFormat, params object[] args)
		{
			switch(logType)
			{
				case Priority.DEBUG:
					Debug(strFormat, args);
					break;

				case Priority.WARN:
					Warn(strFormat, args);
					break;

				case Priority.ERROR:
					Error(strFormat, args);
					break;

				case Priority.INFO:
					Info(strFormat, args);
					break;

				case Priority.FATAL:
					Fatal(strFormat, args);
					break;

				default:
					break;
			}
		}

		/// <summary>
		/// Debug message - with format
		/// </summary>
		/// <param name="strFormat">String format</param>
		/// <param name="args">Parameter list</param>
		public virtual void Debug(string strFormat, params object[] args)
		{
			StringBuilder msgDebug = new StringBuilder();
			
			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if (debugEnabled)
						{
							msgDebug.Append(GetMessage(Priority.DEBUG, ""));
							if ( this.GetParamCount(strFormat) != 0 )
							{
								msgDebug.AppendFormat(strFormat, args);
							}
							else
							{
								msgDebug.Append(strFormat);
							}
						
							debugListener.WriteLine(msgDebug.ToString());
							debugListener.Flush();
						}
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}
		}

		/// <summary>
		/// Debug Message - With exception
		/// </summary>
		/// <param name="obj">Object</param>
		/// <param name="ex">Exception</param>
		public virtual void DebugEx(object obj, Exception ex)
		{
			StringBuilder msgDebug = new StringBuilder();

			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if (debugEnabled)
						{
							msgDebug.Append(GetMessage(Priority.DEBUG, obj, ex));

							debugListener.WriteLine(msgDebug.ToString());
							debugListener.Flush();
						}
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}

		}


		/// <summary>
		/// Error Message - With Format
		/// </summary>
		/// <param name="strFormat">String format</param>
		/// <param name="args">Parameter list</param>
		public virtual void Error(string strFormat, params object[] args)
		{
			StringBuilder msgError = new StringBuilder();

			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if (errorEnabled)
						{
							msgError.Append(GetMessage(Priority.ERROR, ""));
							if (this.GetParamCount(strFormat) != 0)
							{
								msgError.AppendFormat(strFormat, args);					
							}
							else
							{
								msgError.Append(strFormat);
							}

							debugListener.WriteLine(msgError.ToString());
							debugListener.Flush();
						}
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}
		}

		/// <summary>
		/// Error message - With Exception
		/// </summary>
		/// <param name="obj">Object</param>
		/// <param name="ex">Exception</param>
		public virtual void ErrorEx(object obj, Exception ex)
		{
			StringBuilder msgError = new StringBuilder();

			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if (errorEnabled)
						{
							msgError.Append(GetMessage(Priority.DEBUG, obj, ex));
					
							debugListener.WriteLine(msgError.ToString());
							debugListener.Flush();
						}
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}
		}

		/// <summary>
		/// Information Message - With Format
		/// </summary>
		/// <param name="strFormat">String format</param>
		/// <param name="args">Parameter list</param>
		public virtual void Info(string strFormat, params object[] args)
		{
			StringBuilder msgInfo = new StringBuilder();

			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if (infoEnabled)
						{
							msgInfo.Append(GetMessage(Priority.INFO, ""));
							if ( this.GetParamCount(strFormat) != 0 )
							{
								msgInfo.AppendFormat(strFormat, args);					
							}
							else
							{
								msgInfo.Append(strFormat);
							}
							debugListener.WriteLine(msgInfo.ToString());
							debugListener.Flush();
						}
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}
		}

		/// <summary>
		/// Information message - With exception
		/// </summary>
		/// <param name="obj">Object</param>
		/// <param name="ex">Exception</param>
		public virtual void InfoEx(object obj, Exception ex)
		{
			StringBuilder msgInfo = new StringBuilder();

			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if (infoEnabled)
						{
							msgInfo.Append(GetMessage(Priority.DEBUG, obj, ex));
					
							debugListener.WriteLine(msgInfo.ToString());
							debugListener.Flush();
						}
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}
		}

		/// <summary>
		/// Warning message - With format
		/// </summary>
		/// <param name="strFormat">String format</param>
		/// <param name="args">Parameter list</param>
		public virtual void Warn(string strFormat, params object[] args)
		{
			StringBuilder msgWarn = new StringBuilder();

			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if (warnEnabled)
						{
							msgWarn.Append(GetMessage(Priority.WARN, ""));
							if ( this.GetParamCount(strFormat) != 0 )
							{
								msgWarn.AppendFormat(strFormat, args);					
							}
							else
							{
								msgWarn.Append(strFormat);
							}
							debugListener.WriteLine(msgWarn.ToString());
							debugListener.Flush();
						}
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}
		}

		/// <summary>
		/// Warning message - With Exception
		/// </summary>
		/// <param name="obj">Object</param>
		/// <param name="ex">Exception</param>
		public virtual void WarnEx(object obj, Exception ex)
		{
			StringBuilder msgWarn = new StringBuilder();

			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if (warnEnabled)
						{
							msgWarn.Append(GetMessage(Priority.DEBUG, obj, ex));
					
							debugListener.WriteLine(msgWarn.ToString());
							debugListener.Flush();
						}
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}
		}

		/// <summary>
		/// Fatal message - With format
		/// </summary>
		/// <param name="strFormat">String format</param>
		/// <param name="args">Parameter list</param>
		public virtual void Fatal(string strFormat, params object[] args)
		{
			StringBuilder msgFatal = new StringBuilder();

			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if (fatalEnabled)
						{
							msgFatal.Append(GetMessage(Priority.FATAL, ""));
							if ( this.GetParamCount(strFormat) != 0 )
							{
								msgFatal.AppendFormat(strFormat, args);					
							}
							else
							{
								msgFatal.Append(strFormat);
							}
							debugListener.WriteLine(msgFatal.ToString());
							debugListener.Flush();
						}
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}
		}

		/// <summary>
		/// Fatal message - with exception
		/// </summary>
		/// <param name="obj">Object</param>
		/// <param name="ex">Exception</param>
		public virtual void FatalEx(object obj, Exception ex)
		{
			StringBuilder msgFatal = new StringBuilder();
			
			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if (fatalEnabled)
						{
							msgFatal.Append(GetMessage(Priority.DEBUG, obj, ex));
					
							debugListener.WriteLine(msgFatal.ToString());
							debugListener.Flush();
						}
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}
		}

		/// <summary>
		/// Simple Message - With Format
		/// </summary>
		/// <param name="strFormat">String format</param>
		/// <param name="args">Parameter list</param>
		public virtual void Simple(string strFormat, params object[] args)
		{
			StringBuilder msgSimple = new StringBuilder();

			try
			{
				lock(debugListener)
				{
					if (debugListener != null)
					{
						if ( this.GetParamCount(strFormat) != 0 )
						{
							msgSimple.AppendFormat(strFormat, args);					
						}
						else
						{
							msgSimple.Append(strFormat);
						}
						debugListener.WriteLine(msgSimple);
						debugListener.Flush();
					}
				}
			}
			catch (Exception)
			{
				debugListener = null;
			}
		}

		/// <summary>
		/// Gives information about the ability of log certain type of messages		
		/// </summary>
		/// <param name="priority">Message priority</param>
		/// <returns>True or false</returns>
		public bool IsEnabledFor(Priority priority)
		{
			switch(priority)
			{
				case Priority.DEBUG:
					return debugEnabled;

				case Priority.WARN:
					return warnEnabled;

				case Priority.ERROR:
					return errorEnabled;					

				case Priority.INFO:
					return infoEnabled;					

				case Priority.FATAL:
					return fatalEnabled;

				default:
					return false;					
			}
		}

		/// <summary>
		/// Returns a string with a formmated message - Normal
		/// </summary>
		/// <param name="priority">Message priority</param>
		/// <param name="obj"></param>
		/// <returns>a string</returns>
		private string GetMessage(Priority priority, object obj)
		{
			StringBuilder msg = new StringBuilder();

			switch(priority)
			{
				case Priority.DEBUG:
					msg.Append("DEBUG ");
					break;

				case Priority.WARN:
					msg.Append("WARN ");
					break;

				case Priority.ERROR:
					msg.Append("ERROR ");
					break;

				case Priority.INFO:
					msg.Append("INFO ");
					break;

				case Priority.FATAL:
					msg.Append("FATAL ");
					break;

				default:
					break;
			}

			string className	= String.Empty;
			string methodName	= String.Empty;

			StackTrace stack = new StackTrace(0);

			for (int i = 0; i < stack.FrameCount; i++)
			{
				if (stack.GetFrame(i).GetMethod().DeclaringType.Name != "Logger")
				{
					className	= stack.GetFrame(i).GetMethod().DeclaringType.Name;
					methodName	= stack.GetFrame(i).GetMethod().Name;
					break;
				}
			}

			msg.Append("[" + System.DateTime.Now.ToString() + "] ");
			msg.AppendFormat("( {0}.{1} ) ", className, methodName);
			msg.Append(obj.ToString());

			return msg.ToString();
		}

		/// <summary>
		/// Returns a string with a formmated message - With exception
		/// </summary>
		/// <param name="priority">Message priority</param>
		/// <param name="obj">Object</param>
		/// <param name="ex">Exception</param>
		/// <returns>a string</returns>
		private string GetMessage(Priority priority, object obj, Exception ex)
		{
			StringBuilder msg = new StringBuilder();

			msg.Append(GetMessage(priority,obj) + "\n");
			msg.Append("\t\t");
			msg.Append("EXCEPTION [InnerException]: " + ex.InnerException + "\n");
			msg.Append("\t\t\t\t");
			msg.Append("[Source]: " + ex.Source + "\n");
			msg.Append("\t\t\t\t");
			msg.Append("[StackTrace]: " + ex.StackTrace + "\n");
			
			return msg.ToString();
		}

		/// <summary>
		/// Returns the number of parameters for the message template.
		/// </summary>
		/// <returns></returns>
		public int GetParamCount(string strFormat) 
		{
			int count = 0;
			for(int i = 0; i < strFormat.Length; i++)
			{
				if (strFormat[i] == '{') 
				{
					count++;
				}
			}

			return count;
		}

		#endregion
	}	
}