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
				
		private bool			autoCommit = true;
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

		public bool AutoCommit 
		{
			get {return autoCommit;}
			set {autoCommit = value;}
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
				// Firebird don´t have clustered indexes
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
			// ADO.NET interfaces don´t support database creation
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

				if (autoCommit)
				{
					TransactionCommit();
				}
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
				TransactionBegin();

				command = GetCommand(stg);

				command.ExecuteNonQuery();

				if (autoCommit)
				{
					TransactionCommit();
				}
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

		public int Load()
		{
			try
			{
				TransactionBegin();
				loadFile("updates");
				TransactionCommit();

				TransactionBegin();
				loadFile("hundred");
				TransactionCommit();
				
				TransactionBegin();
				loadFile("tenpct");
				TransactionCommit();
				
				TransactionBegin();
				loadFile("uniques");
				TransactionCommit();
				
				TransactionBegin();
				loadTinyFile("tiny");
				TransactionCommit();
			}
			catch (Exception ex)
			{
				TransactionRollback();
				log.Error("load failed!!");
				throw ex;
			}

			return -1;			
		}

		private void loadFile(string table)
		{
			StringBuilder	commandText = new StringBuilder();
			StreamReader	stream		= null;
			IDbCommand		command		= null;

			commandText.AppendFormat("insert into {0} values (?,?,?,?,?,?,?,?,?,?)", table);

			/* Crate command */
			command = GetCommand(commandText.ToString());

			/* Add parameters	*/
			command.Parameters.Add(GetParameter("@col_key", "COL_KEY"));
			command.Parameters.Add(GetParameter("@col_int", "COL_INT"));
			command.Parameters.Add(GetParameter("@col_signed", "COL_SIGNED"));
			command.Parameters.Add(GetParameter("@col_float", "COL_FLOAT"));
			command.Parameters.Add(GetParameter("@col_double", "COL_DOUBLE"));
			command.Parameters.Add(GetParameter("@col_decim", "COL_DECIM"));
			command.Parameters.Add(GetParameter("@col_date", "COL_DATE"));
			command.Parameters.Add(GetParameter("@col_code", "COL_CODE"));
			command.Parameters.Add(GetParameter("@col_name", "COL_NAME"));
			command.Parameters.Add(GetParameter("@col_address", "COL_ADDRESS"));

			/* Prepare command execution	*/
			command.Prepare();

			stream = new StreamReader(
					(System.IO.Stream)File.Open(
												dataPath + "asap." + table	,
												FileMode.Open				,
												FileAccess.Read				,
												FileShare.None));

			while (stream.Peek() > -1)
			{
				string[] elements = stream.ReadLine().Split(',');
			
				for (int i = 0; i < 10; i++)
				{
					((IDataParameter)command.Parameters[i]).Value = elements[i];
				}
	
				command.ExecuteNonQuery();
			}

			command.Dispose();
			stream.Close();
		}


		private void loadTinyFile(string table)
		{
			StringBuilder	commandText = new StringBuilder();
			StreamReader	stream		= null;
			IDbCommand		command		= null;

			// commandText.AppendFormat("insert into {0} values (?)", table);
			commandText.AppendFormat("insert into {0} values (@col_key)", table);

			/* Crate command */
			command = GetCommand(commandText.ToString());

			/* Add parameters	*/
			command.Parameters.Add(GetParameter("@col_key", "COL_KEY"));

			/* Prepare command execution	*/
			command.Prepare();

			stream = new StreamReader(
				(System.IO.Stream)File.Open(
				dataPath + "asap." + table	,
				FileMode.Open				,
				FileAccess.Read				,
				FileShare.None));

			while (stream.Peek() > -1)
			{
				string[] elements = stream.ReadLine().Split(',');
							
				((IDataParameter)command.Parameters[0]).Value = elements[0];
	
				command.ExecuteNonQuery();
			}

			stream.Close();
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

		#endregion
	}
}