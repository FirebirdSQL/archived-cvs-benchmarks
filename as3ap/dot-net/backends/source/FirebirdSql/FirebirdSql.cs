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
using AS3AP.LogData;

using FirebirdSql.Data.Firebird;

namespace AS3AP.BenchMark.Backends
{
	public class FirebirdSql : IBackend
	{
		#region FIELDS

		private bool			autoCommit = true;
		private IsolationLevel	isolation  = IsolationLevel.ReadCommitted;
		private Logger			log;
		private string			dataPath;
		private string			connectionString;
		private FbConnection	connection;
		private FbTransaction	transaction;
		private FbDataReader	cursor;
		private FbCommand		cmdCursor;	

		#endregion

		#region PROPERTIES

		IDataReader IBackend.Cursor
		{
			get { return Cursor; }
		}
		
		public FbDataReader Cursor
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
	
		public FirebirdSql()
		{
			getConfiguration();
			
			log = new Logger(GetType(), ConfigurationSettings.AppSettings["LogFile"], Mode.OVERWRITE);
		}

		#endregion

		#region METHODS

		private void getConfiguration()
		{
			dataPath			= ConfigurationSettings.AppSettings["DataPath"];
			connectionString	= ConfigurationSettings.AppSettings["ConnectionString"];
			if (!dataPath.EndsWith(Path.DirectorySeparatorChar.ToString()))
			{
				dataPath += Path.DirectorySeparatorChar;
			}
		}

		public int CountTuples(string table)
		{
			FbCommand		command		= null;
			StringBuilder	commandText = new StringBuilder();
			int				count;

			commandText.AppendFormat("select count(col_key) from {0}", table);
			
			try
			{
				TransactionBegin();

				command = GetCommand(commandText.ToString());
					// new FbCommand(commandText.ToString(), connection, transaction);
				count = (int)command.ExecuteScalar();

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
			try
			{
				connection = new FbConnection(connectionString);
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
			FbCommand command = null;

			try
			{
				TransactionBegin();

				command = GetCommand(stg);
					// new FbCommand(stg, connection, transaction);

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
			FbCommand command = null;

			try
			{
				TransactionBegin();

				command = GetCommand(stg);
					// new FbCommand(stg, connection, transaction);

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
				loadFile("updates");
				loadFile("hundred");
				loadFile("tenpct");			
				loadFile("uniques");
				loadTinyFile("tiny");
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
			int				count		= 0;
			StringBuilder	commandText = new StringBuilder();
			StreamReader	stream		= null;
			FbCommand		command		= null;

			TransactionBegin();

			commandText.AppendFormat(
					"insert into {0} values (@col_key, @col_int, @col_signed, @col_float, @col_double, @col_decim, @col_date, @col_code, @col_name, @col_address)", table);

			/* Crate command */
			command = GetCommand(commandText.ToString());

			/* Add parameters	*/
			command.Parameters.Add("@col_key"	, FbType.Integer, "COL_KEY");
			command.Parameters.Add("@col_int"	, FbType.Integer, "COL_INT");
			command.Parameters.Add("@col_signed", FbType.Integer, "COL_SIGNED");
			command.Parameters.Add("@col_float"	, FbType.Float	, "COL_FLOAT");
			command.Parameters.Add("@col_double", FbType.Double	, "COL_DOUBLE");
			command.Parameters.Add("@col_decim"	, FbType.Decimal, "COL_DECIM");
			command.Parameters.Add("@col_date"	, FbType.Char	, "COL_DATE");
			command.Parameters.Add("@col_code"	, FbType.Char	, "COL_CODE");
			command.Parameters.Add("@col_name"	, FbType.Char	, "COL_NAME");
			command.Parameters.Add("@col_address", FbType.VarChar, "COL_ADDRESS");

			/* Prepare command execution	*/
			command.Prepare();

			stream = new StreamReader((System.IO.Stream)File.Open(
										dataPath + "asap." + table	,
										FileMode.Open				,
										FileAccess.ReadWrite		,
										FileShare.None), Encoding.ASCII);

			while (stream.Peek() > -1)
			{
				string[] elements = stream.ReadLine().Split(',');
			
				for (int i = 0; i < 10; i++)
				{
					command.Parameters[i].Value = elements[i];
				}
	
				command.ExecuteNonQuery();

				if (count < 10000)
				{
					count++;
				}
				else
				{
					count = 0;

					/* Commit work after 500 records	*/
					TransactionCommit();

					/* Begin a new transaction	*/
					TransactionBegin();

					/* Assign the new transaction to the command	*/
					command.Transaction = transaction;
				}
			}

			TransactionCommit();

			command.Dispose();
			stream.Close();
		}


		private void loadTinyFile(string table)
		{
			StringBuilder	commandText = new StringBuilder();
			StreamReader	stream		= null;
			FbCommand		command		= null;

			TransactionBegin();

			commandText.AppendFormat("insert into {0} values (?)", table);

			/* Crate command */
			command = GetCommand(commandText.ToString());

			/* Add parameters	*/
			command.Parameters.Add("@col_key", FbType.Integer, "COL_KEY");

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
			
				command.Parameters[0].Value = elements[0];
	
				command.ExecuteNonQuery();
			}

			TransactionCommit();

			command.Dispose();
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

		public FbCommand GetCommand(string commandText)
		{
			return new FbCommand(commandText, connection, transaction);
		}

		#endregion
	}
}