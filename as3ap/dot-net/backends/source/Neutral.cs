//
// AS3AP -An ANSI SQL Standard Scalable and Portable Benchmark
// for Relational Database Systems.
//
// Ported from OSDB project at http://osdb.sourceforge.net
//
// Author: Carlos Guzmán Álvarez <carlosga@telefonica.net>
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

using CSharp.Logger;

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

		private long			dataSize;

		#endregion

		#region PROPERTIES

		public long DataSize
		{
			get { return dataSize; }
			set { dataSize = value; }
		}

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
			
			log = new Logger("AS3AP_ERRORS.LOG", Mode.OVERWRITE);
		}

		#endregion

		#region METHODS

		public void CloseLogger()
		{
			log.Close();
		}

		private void getConfiguration()
		{
			connectionString	= ConfigurationSettings.AppSettings["ConnectionString"];
			dataPath			= ConfigurationSettings.AppSettings["DataPath"];
			if (!dataPath.EndsWith(Path.DirectorySeparatorChar.ToString()))
			{
				dataPath += Path.DirectorySeparatorChar;
			}

			connectionClass = ConfigurationSettings.AppSettings["BackendConnectionClass"];
			commandClass	= ConfigurationSettings.AppSettings["BackendCommandClass"];
			parameterClass	= ConfigurationSettings.AppSettings["BackendParameterClass"];

			try
			{
				assembly = Assembly.Load(ConfigurationSettings.AppSettings["BackendAssembly"]);
			}
			catch(Exception)
			{
				try
				{
					assembly = Assembly.LoadWithPartialName(ConfigurationSettings.AppSettings["BackendAssembly"]);
				}
				catch (Exception)
				{
				}
			}
		}

		public void CreateIndexBtree(string indexName, string tableName, string fields)
		{
			StringBuilder commandText = new StringBuilder();

			commandText.AppendFormat("create index {0} on {1} ({2})",
				indexName, tableName, fields);

			try
			{
				TransactionBegin();
				ExecuteStatement(commandText.ToString());
				TransactionCommit();
			}
			catch(Exception ex)
			{
				log.Error("btree error {0}", ex.Message);
				throw ex;				
			}
		}

		public void CreateIndexCluster(string indexName, string tableName, string fields)
		{
			StringBuilder commandText = new StringBuilder();

			commandText.AppendFormat("create unique index {0} on {1} ({2})",
				indexName, tableName, fields);

			try
			{
				/* This is not needed, Firebird creates a new unique index
				 * when a table has a primary key defined.
				 */
				// ExecuteStatement(commandText.ToString());
			}
			catch(Exception ex)
			{
				log.Error("cluster error {0}", ex.Message);
				throw ex;
			}
		}

		public void CreateForeignKey(string foreignTable, string constraintName, 
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
				TransactionBegin();
				ExecuteStatement(commandText.ToString());
				TransactionCommit();
			}
			catch(Exception ex)
			{
				log.Error("foreign key error {0}", ex.Message);
				throw ex;
			}
		}

		public void CreateIndexHash(string indexName, string tableName, string fields)
		{
			StringBuilder commandText = new StringBuilder();

			commandText.AppendFormat("create index {0} on {1} ({2})",
				indexName, tableName, fields);

			try
			{
				TransactionBegin();
				// Firebird doesn't have clustered indexes
				ExecuteStatement(commandText.ToString());
				TransactionCommit();
			}
			catch (Exception ex)
			{
				log.Error("btree error {0}", ex.Message);
				throw ex;
			}
		}

		public void CreateTable(string tableName, string tableStructure, string primaryKey) 
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

				TransactionBegin();
				ExecuteStatement(commandText.ToString());
				TransactionCommit();
			}
			catch (Exception ex)
			{
				log.Error("error create table {0}", ex.Message);
				throw ex;
			}
		}

		public void CursorOpen(string stg)
		{
			try
			{				
				cmdCursor	= GetCommand(stg);
				cursor		= cmdCursor.ExecuteReader();
			}
			catch (Exception ex)
			{
				if (log != null) log.Error("CursorOpen failed {0}", ex.Message);

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
				log.Error("connection error", ex.Message);
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
				log.Error("disconnect error", ex.Message);
				throw ex;
			}
		}

		public void ExecuteStatement(string stg)
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

		public void LoadData()
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
				log.Error("load failed {0}", ex.Message);
				throw ex;
			}
		}

		private void loadFile(string table)
		{
			StringBuilder	commandText = new StringBuilder();
			StreamReader	stream		= null;
			IDbCommand		command		= null;

			commandText.AppendFormat("insert into {0} values (@col_key,@col_int,@col_signed,@col_float,@col_double,@col_decim,@col_date,@col_code,@col_name,@col_address)", table);

			/* Create command */
			command = createCommand(commandText.ToString());

			/* Add parameters	*/
			command.Parameters.Add(GetParam("@col_key", DbType.Int32, 4, 0, 0));
			command.Parameters.Add(GetParam("@col_int", DbType.Int32, 4, 0, 0));
			command.Parameters.Add(GetParam("@col_signed", DbType.Int32, 4, 0, 0));
			command.Parameters.Add(GetParam("@col_float", DbType.Single, 8, 0, 0));
			command.Parameters.Add(GetParam("@col_double", DbType.Double, 8, 0, 0));
			command.Parameters.Add(GetParam("@col_decim", DbType.Decimal, 9, 18, 2));
			command.Parameters.Add(GetParam("@col_date", DbType.StringFixedLength, 20, 0, 0));
			command.Parameters.Add(GetParam("@col_code", DbType.StringFixedLength, 10, 0, 0));
			command.Parameters.Add(GetParam("@col_name", DbType.StringFixedLength, 20, 0, 0));
			command.Parameters.Add(GetParam("@col_address", DbType.String, 80, 0, 0));

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
					((IDbDataParameter)command.Parameters[i]).Value = elements[i];
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

			commandText.AppendFormat("insert into {0} values (@col_key)", table);

			/* Create command */
			command = createCommand(commandText.ToString());

			/* Add parameters */
			command.Parameters.Add(GetParam("@col_key", DbType.Int32, 4, 0, 0));

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
			
				((IDbDataParameter)command.Parameters[0]).Value = elements[0];
	
				command.ExecuteNonQuery();
			}

			stream.Close();
		}

		public void CreateData()
		{
		}

		private IDbCommand createCommand(string commandText)
		{
			object[] parameters = new object[3];

			parameters[0] = commandText;
			parameters[1] = connection;
			parameters[2] = transaction;

			return (IDbCommand)Activator.CreateInstance(
											assembly.GetType(commandClass), 
											parameters);
		}

		private IDataParameter GetParam(string parameterName, DbType parameterType, int size, byte precision, byte scale)
		{
			object[] parameters = new object[1];

			parameters[0] = parameterName;

			IDataParameter parameter =  (IDataParameter)Activator.CreateInstance(
										assembly.GetType(this.parameterClass));

			parameter.ParameterName = parameterName;
			parameter.DbType		= parameterType;
			((IDbDataParameter)parameter).Size = size;
			if (parameter.DbType == DbType.Decimal)				
			{
				((IDbDataParameter)parameter).Precision = precision;
				((IDbDataParameter)parameter).Scale		= scale;
			}

			return parameter;
		}

		#endregion
	}
}