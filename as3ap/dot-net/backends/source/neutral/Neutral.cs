//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
//
// Ported from OSDB project at http://osdb.sourceforge.net
//
// Author: Carlos Guzm�n �lvarez <carlosga@telefonica.net>
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
using System.IO;
using System.Data;
using System.Text;
using System.Configuration;
using System.Reflection;

using AS3AP.LogData;

using FirebirdSql.Data.Firebird;

namespace AS3AP.BenchMark.Backends
{
	public class Neutral : IBackend
	{
		#region FIELDS

		private Assembly		assembly;

		private string			connectionClass;
		private string			commandClass;
		private string			parameterClass;
						
		private IsolationLevel	isolation  = IsolationLevel.ReadCommitted;
		private Logger			log;
		private string			dataPath;
		private string			connectionString;
		private IDbConnection	connection;
		private IDbTransaction	transaction;
		private IDataReader		cursor;
		private IDbCommand		cmdCursor;	

		#endregion

		#region PROPERTIES

		IDataReader IBackend.Cursor
		{
			get { return Cursor; }
		}
		
		public IDataReader Cursor
		{
			get { return cursor; }
		}

		public IsolationLevel Isolation
		{
			get {return isolation;}
			set {isolation = value;}
		}

		#endregion

		#region CONSTRUCTORS
	
		public Neutral()
		{
			getConfiguration();
			
			log = new Logger(GetType(), ConfigurationSettings.AppSettings["LogFile"], Mode.OVERWRITE);
		}

		#endregion

		#region METHODS

		private void getConfiguration()
		{
			connectionString	= ConfigurationSettings.AppSettings["ConnectionString"];
			dataPath = ConfigurationSettings.AppSettings["DataPath"];
			if (!dataPath.EndsWith(Path.DirectorySeparatorChar.ToString()))
			{
				dataPath += Path.DirectorySeparatorChar;
			}

			assembly = Assembly.Load(ConfigurationSettings.AppSettings["BackendAssembly"]);

			connectionClass = ConfigurationSettings.AppSettings["BackendConnectionClass"];
			commandClass	= ConfigurationSettings.AppSettings["BackendCommandClass"];
			parameterClass	= ConfigurationSettings.AppSettings["BackendParameterClass"];
		}

		public int CountTuples(string table)
		{
			IDbCommand		command		= null;
			StringBuilder	commandText = new StringBuilder();
			int				count;

			commandText.AppendFormat("select count(col_key) from {0}", table);
			
			try
			{
				TransactionBegin();

				command = GetCommand(commandText.ToString());
				count	= (int)command.ExecuteScalar();

				TransactionCommit();
			}
			catch(Exception ex)
			{
				TransactionRollback();
				log.Error("Error counting tuples");
				throw ex;
			}
			finally
			{
				command.Dispose();
			}

			return count;
		}

		public void CreateIndexBtree(string iName, string tName, string fields)
		{
			StringBuilder	commandText = new StringBuilder();

			commandText.AppendFormat("create index {0} on {1} ({2})",
									iName, tName, fields);

			try
			{
				ddl(commandText.ToString());
			}
			catch(Exception ex)
			{
				log.Error("btree error");
				throw ex;				
			}
		}

		public void CreateIndexCluster(string iName, string tName, string fields)
		{
			StringBuilder	commandText = new StringBuilder();

			commandText.AppendFormat("create unique index {0} on {1} ({2})",
									iName, tName, fields);

			try
			{
				// Firebird no tiene soporte para indices clustered
				ddl(commandText.ToString());
			}
			catch(Exception ex)
			{
				log.Error("cluster error");
				throw ex;
			}
		}

		public void CreateIndexForeign(string tName, string keyName, string keyCol,
										string fTable, string fFields)
		{
			StringBuilder	commandText = new StringBuilder();

			commandText.AppendFormat("alter table {0} add constraint {1} foreign key ({2}) references {3} ({4}) {5} {6}",
				tName, keyName, keyCol, fTable, fFields,
				"on delete cascade", "on update cascade");

			try
			{
				ddl(commandText.ToString());
			}
			catch(Exception ex)
			{
				log.Error("foreign key error");
				throw ex;
			}
		}

		public void CreateIndexHash(string iName, string tName, string fields)
		{
			StringBuilder	commandText = new StringBuilder();

			commandText.AppendFormat("create index {0} on {1} ({2})",
									iName, tName, fields);

			try
			{
				// Firebird don�t have clustered indexes
				ddl(commandText.ToString());
			}
			catch (Exception ex)
			{
				log.Error("btree error");
				throw ex;
			}
		}

		public void CreateTable(string stg) 
		{
			try
			{
				ddl(stg);
			}
			catch (Exception ex)
			{
				log.Error("error create table");
				throw ex;
			}
		}


		public void CursorOpen(string stg)
		{
			try
			{
				TransactionBegin();
				
				cmdCursor	= GetCommand(stg);
				cursor		= cmdCursor.ExecuteReader();
			}
			catch (Exception ex)
			{
				TransactionRollback();

				if (cursor != null)
				{
					cursor.Dispose();
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


		public bool CursorFetch()
		{
			return cursor.Read();
		}

		
		public void CursorClose()
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
				throw ex;
			}
			finally
			{
				TransactionCommit();

				if (cursor != null)
				{
					cursor.Dispose();
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
			object[] parameters = new object[1];

			try
			{
				parameters[0] = connectionString;
				connection = (IDbConnection)Activator.CreateInstance(
										assembly.GetType(connectionClass),
										parameters);
				connection.Open();
			}
			catch (Exception ex)
			{
				log.Error("connection error");
				throw ex;
			}
		}

		public void DatabaseCreate(string dName)
		{
			// ADO.NET interfaces don�t support database creation
		}

		public void DatabaseDisconnect()
		{
			try
			{
				if (connection != null)
				{
					connection.Close();
				}
			}
			catch (Exception ex)
			{
				log.Error("disconnect error");
				throw ex;
			}
		}

		public void ddl(string stg)
		{			
			IDbCommand command = null;

			try
			{
				TransactionBegin();

				command = GetCommand(stg);
				command.ExecuteNonQuery();

				TransactionCommit();
			}
			catch (Exception ex)
			{
				TransactionRollback();
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

		public void dml(string stg)
		{
			IDbCommand command = null;

			try
			{
				command = GetCommand(stg);
				command.ExecuteNonQuery();
			}
			catch (Exception ex)
			{
				TransactionRollback();
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


		public void TransactionBegin()
		{
			try
			{
				transaction = connection.BeginTransaction(isolation);
			}
			catch(Exception ex)
			{
				Console.WriteLine("transaccion no iniciada");
				throw ex;
			}
		}


		public void TransactionCommit()
		{
			try
			{
				transaction.Commit();
			}
			catch (Exception ex)
			{				
				throw ex;
			}
		}


		public void TransactionRollback()
		{
			try
			{
				transaction.Rollback();
			}
			catch (Exception ex)
			{
				throw ex;
			}
		}


		private IDbCommand GetCommand(string commandText)
		{
			IDbCommand command = (IDbCommand)Activator.CreateInstance(
									assembly.GetType(commandClass));
			
			command.CommandText = commandText;
			command.Connection  = connection;
			command.Transaction = transaction;			

			return command;
		}


		private IDataParameter GetParameter(string parameterName, string sourceColumn)
		{
			IDataParameter param = (IDataParameter)Activator.CreateInstance(
												assembly.GetType(parameterClass));


			param.ParameterName = parameterName;
			param.SourceColumn	= sourceColumn;

			return param;
		}


		public int CreateData(long dataSize)
		{
			return 0;
		}

		#endregion
	}
}