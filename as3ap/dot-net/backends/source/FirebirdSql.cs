//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
//
// Ported from OSDB project at http://osdb.sourceforge.net
//
// Author: Carlos Guzmn lvarez <carlosga@telefonica.net>
//
// Distributable under GPL license.
// You may obtain a copy of the License at http://www.gnu.org/copyleft/gpl.html
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
using System.Globalization;

using CSharp.Logger;

using FirebirdSql.Data.Firebird;

namespace AS3AP.BenchMark.Backends
{
	public class FirebirdSql : IBackend
	{
		#region CONSTANTS
		
		private const int HUNDREDMILLION	= 10*10*10*10*10*10*10*10;
		private const int THOUSANDMILLION	= HUNDREDMILLION*10;
		
		#endregion

		#region FIELDS

		private NumberFormatInfo numberFormat = new NumberFormatInfo();

		private IsolationLevel	isolation  = IsolationLevel.ReadCommitted;
		private Logger			log;		
		private string			connectionString;
		private FbConnection	connection;
		private FbTransaction	transaction;
		private FbDataReader	cursor;
		private FbCommand		cmdCursor;	

		private long			dataSize;

		private string			dataPath = String.Empty;

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
		
		public FbDataReader Cursor
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
	
		public FirebirdSql()
		{
			getConfiguration();

			try
			{
				log = new Logger("AS3AP_ERRORS.LOG", Mode.OVERWRITE);
			}
			catch (Exception)
			{
				try
				{
					log = new Logger("AS3AP_ERRORS.LOG", Mode.APPEND);
				}
				catch (Exception)
				{
					log = null;
				}
			}

			// Use point as decimal separator
			numberFormat.CurrencyDecimalSeparator = ".";
		}

		#endregion

		#region METHODS

		public void CloseLogger()
		{
			if (log != null) log.Close();
		}

		private void getConfiguration()
		{
			connectionString	= ConfigurationSettings.AppSettings["ConnectionString"];
			dataPath			= ConfigurationSettings.AppSettings["DataPath"];

			if (!dataPath.EndsWith(Path.DirectorySeparatorChar.ToString()))
			{
				dataPath += Path.DirectorySeparatorChar;
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
				if (log != null) log.Error("btree error {0}", ex.Message);
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
				if (log != null) log.Error("cluster error {0}", ex.Message);
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
				if (log != null) log.Error("foreign key error {0}", ex.Message);
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
				if (log != null) log.Error("btree error {0}", ex.Message);
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
				if (log != null) log.Error("error create table {0}", ex.Message);
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
				TransactionRollback();
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
			try
			{
				connection = new FbConnection(connectionString);
				connection.Open();
			}
			catch (Exception ex)
			{
				if (log != null) log.Error("connection error {0}", ex.Message);
				throw ex;
			}
		}

		public void DatabaseCreate(string dName)
		{
			// ADO.NET interfaces don't support database creation
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
				if (log != null) log.Error("disconnect error {0}", ex.Message);
				throw ex;
			}
		}

		public void ExecuteStatement(string statement)
		{
			FbCommand command = null;

			try
			{
				command = GetCommand(statement);
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
			finally
			{
				transaction = null;
			}
		}

		public void TransactionRollback()
		{
			try
			{
				transaction.Rollback();
				transaction = null;
			}
			catch (Exception ex)
			{				
				throw ex;
			}
			finally
			{
				transaction = null;
			}
		}

		public FbCommand GetCommand(string commandText)
		{
			return new FbCommand(commandText, connection, transaction);
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
				if (log != null) log.Error("load failed {0}", ex.Message);
				throw ex;
			}
		}

		private void loadFile(string table)
		{
			StringBuilder	commandText = new StringBuilder();
			StreamReader	stream		= null;
			FbCommand		command		= null;

			commandText.AppendFormat("insert into {0} values (?,?,?,?,?,?,?,?,?,?)", table);

			/* Crate command */
			command = new FbCommand(commandText.ToString(), connection, transaction);

			/* Add parameters	*/
			command.Parameters.Add("@col_key"	, FbType.Integer	, "COL_KEY");
			command.Parameters.Add("@col_int"	, FbType.Integer	, "COL_INT");
			command.Parameters.Add("@col_signed", FbType.Integer	, "COL_SIGNED");
			command.Parameters.Add("@col_float"	, FbType.Float		, "COL_FLOAT");
			command.Parameters.Add("@col_double", FbType.Double		, "COL_DOUBLE");
			command.Parameters.Add("@col_decim"	, FbType.Decimal	, "COL_DECIM");
			command.Parameters.Add("@col_date"	, FbType.Char		, "COL_DATE");
			command.Parameters.Add("@col_code"	, FbType.Char		, "COL_CODE");
			command.Parameters.Add("@col_name"	, FbType.Char		, "COL_NAME");
			command.Parameters.Add("@col_address", FbType.VarChar	, "COL_ADDRESS");

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
					command.Parameters[i].Value = elements[i];
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
			FbCommand		command		= null;

			commandText.AppendFormat("insert into {0} values (?)", table);

			/* Crate command */
			command = new FbCommand(commandText.ToString(), connection, transaction);

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

			stream.Close();
		}

		public void CreateData()
		{
			string col_address				= String.Empty;
			string col_code					= String.Empty;
			string col_name					= String.Empty;
			string date_string				= String.Empty;
			string hundred_address			= String.Empty;
			string hundred_name				= String.Empty;
			string[] hundred_unique_address	= new string[100];
			string[] hundred_unique_code	= new string[100];
			string[] hundred_unique_name	= new string[100];
			string name						= String.Empty;

			float[] hundred_unique_float = new float[100];
			float  uniform100_dense;
			float  uniform100_float;
			float[] zipf10 = new float[10];
			float  zipf10_float;
			float[] zipf100 = new float[100];
			float zipf100_float;
			
			long i;
			long rec;
			long date_random;
			long dense_key;
			long hundred_key;			
			long randomizer = 0;
			long r10pct_key = 0;
			long sparse_key;
			long sparse_signed;
			long sparse_key_spread;
			long sparse_signed_spread;
			long tenpct;
		        
			double col_double;
			double double_normal;
			double[] hundred_unique_double = new double[100];
			
			DateTime		col_date	= new DateTime();						
			StringBuilder	sqlCommand	= new StringBuilder();
			Random			randNumber	= new Random();
		
			FbDataAdapter	adapter = null;
			FbCommand		command = null;
			DataSet			dataset = null;

			/* These characters can be used without hassle in comma-separated-value files */
			string csv_safe_chars = "#%&()[]{};:/~@ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-=";
			// string csv_safe_chars = "#%&()[]{}:_/~@ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-=";
		    
			int Nlen;				

			try
			{
				/* For our Zipfian distributions, we'll generate values that occur
				 * most often at Zipf[0], and decay across an asymptotic curve to
				 * the value at zipf[RANKS_zipfian-1].  (If someone has a better
				 * algorithm for generating better distributions, please submit it!)
				 */
				for (i = 0; i < 10; i++)
				{
					zipf10[i] = Convert.ToSingle(randNumber.Next(-5*(HUNDREDMILLION), 5*(HUNDREDMILLION)));
				}
				for (i = 0; i < 100; i++)
				{
					zipf100[i] = Convert.ToSingle(randNumber.Next(-5*(HUNDREDMILLION), 5*(HUNDREDMILLION)));
				}
		
				tenpct = dataSize/10;
				if ((sparse_key_spread = (THOUSANDMILLION)/dataSize) < 1)
				{
					sparse_key_spread = 1;
				}
				if ((sparse_signed_spread = (10*(HUNDREDMILLION))/dataSize) < 1)
				{
					sparse_signed_spread = 1;
				}
		    				
				CreateTable(
					"random_data",
					" randomizer int not null,"					+
					" sparse_key int not null,"					+
					" dense_key	int not null,"					+
					" sparse_signed int not null,"				+
					" uniform100_dense int not null,"			+
					" zipf10_float float not null,"				+
					" zipf100_float float not null,"			+
					" uniform100_float float not null,"			+
					" double_normal double precision not null,"	+
					" r10pct_key integer not null,"				+
					" col_date date not null,"					+
					" code char(10) not null,"					+
					" name char(20) not null,"					+
					" address varchar(800) not null", null);
		    	
				CreateTable(
					"random_tenpct"									,
					" col_key 		int not null,"					+
					" col_float		float not null,"				+
					" col_signed 	int not null,"					+
					" col_double 	double precision not null," 	+
					" col_address	varchar(800) not null", null);
			
				TransactionBegin();

				FbCommand cmdRandomData = new FbCommand();

				cmdRandomData.CommandText = "INSERT INTO random_data ("												+
					" randomizer, sparse_key, dense_key, sparse_signed, uniform100_dense,"+
					" zipf10_float, zipf100_float, uniform100_float, double_normal,"	+
					" r10pct_key, col_date, code, name, address)"						+
					" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			
				cmdRandomData.Connection	= connection;
				cmdRandomData.Transaction	= transaction;

				cmdRandomData.Parameters.Add("@randomizer", FbType.Integer);
				cmdRandomData.Parameters.Add("@sparse_key", FbType.Integer);
				cmdRandomData.Parameters.Add("@dense_key", FbType.Integer);
				cmdRandomData.Parameters.Add("@sparse_signed", FbType.Integer);
				cmdRandomData.Parameters.Add("@uniform100_dense", FbType.Float);
				cmdRandomData.Parameters.Add("@zipf10_float", FbType.Float);
				cmdRandomData.Parameters.Add("@zipf100_float", FbType.Float);
				cmdRandomData.Parameters.Add("@uniform100_float", FbType.Float);
				cmdRandomData.Parameters.Add("@double_normal", FbType.Double);
				cmdRandomData.Parameters.Add("@r10pct_key", FbType.Integer);
				cmdRandomData.Parameters.Add("@col_date", FbType.Date);
				cmdRandomData.Parameters.Add("@col_code", FbType.VarChar);
				cmdRandomData.Parameters.Add("@col_name", FbType.VarChar);
				cmdRandomData.Parameters.Add("@col_address", FbType.VarChar);

				cmdRandomData.Prepare();

				for (rec = 1; rec <= dataSize; rec++)
				{
					int Drec;
		    	
					randomizer 			= randNumber.Next(0, THOUSANDMILLION);
					dense_key  			= (rec == 1) ? 0 : rec;       
					sparse_key 			= dense_key * sparse_key_spread;
					sparse_signed 		= (-5*(HUNDREDMILLION)) + ((dense_key) * sparse_signed_spread);
					uniform100_dense 	= 100 + (rec % 100);
					zipf10_float 		= zipf10[randNumber.Next(0, (int)(rec % 10))];
					zipf100_float 		= zipf100[randNumber.Next(0, (int)(rec % 100))];
					uniform100_float 	= 100 + (float)((rec % 100));
					double_normal 		= (double)randNumber.Next(-(THOUSANDMILLION), (THOUSANDMILLION));

					/* To ensure uniqueness, we'll start by generating the record number
					 * in base (Ncsv_safe_chars), followed by "_". We'll then fill out
					 * the field with additional randomly selected characters. (By writing
					 * the digits backwards, we should help to keep the data disorderly :)
					 */
					Drec		= (int)rec;
					col_code	= String.Empty;
					col_name	= String.Empty;
					col_address = String.Empty;
					while (Drec > 0) 
					{
						col_code	+= csv_safe_chars[Drec % csv_safe_chars.Length];
						Drec		/= csv_safe_chars.Length;
					}
					col_code += '_';
					for (i = col_code.Length; i < 10; i++)
					{
						col_code += csv_safe_chars[randNumber.Next(0, csv_safe_chars.Length)];
					}
					col_name = col_code;
					for (i = col_code.Length; i < 20; i++)
					{
						col_name += csv_safe_chars[randNumber.Next(0, csv_safe_chars.Length)];
					} 
					col_address = col_code;
					Nlen = randNumber.Next(2, (int)(6 + (25 * (rec & 3))));
					for (i = col_code.Length; i < Nlen; i++)
					{
						col_address += csv_safe_chars[randNumber.Next(0, csv_safe_chars.Length)];
					}
					if (++r10pct_key > tenpct)
					{
						r10pct_key = 0;
					} 
					else if (r10pct_key == 1) 
					{
						r10pct_key++;
					} 
					try
					{
						/* roughly 36,835 days from 1/1/1900-12/1/2000 */
						date_random = dense_key % 36835;
						/* ignore leap year considerations */
						col_date.AddYears((int)(date_random / 365));
						date_random = (date_random % 365) + 1;
						if (date_random <= 31) 
						{
							col_date.AddMonths(0);
							col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 28) 
						{
							col_date.AddMonths(1);
							col_date.AddDays(date_random);
						} 
						else if ((date_random -= 28) <= 31) 
						{
							col_date.AddMonths(2);
							col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 30) 
						{
							col_date.AddMonths(3);
							col_date.AddDays(date_random);
						} 
						else if ((date_random -= 30) <= 31) 
						{
							col_date.AddMonths(4);
							col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 30) 
						{
							col_date.AddMonths(5);
							col_date.AddDays(date_random);
						} 
						else if ((date_random -= 30) <= 31) 
						{
							col_date.AddMonths(6);
							col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 31) 
						{
							col_date.AddMonths(7);
							col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 30) 
						{
							col_date.AddMonths(8);
							col_date.AddDays(date_random);
						} 
						else if ((date_random -= 30) <= 31) 
						{
							col_date.AddMonths(9);
							col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 30) 
						{
							col_date.AddMonths(10);
							col_date.AddDays(date_random);
						} 
						else 
						{
							col_date.AddMonths(11);
							col_date.AddDays(date_random);
						}
					}
					catch (Exception ex)
					{
						if (log != null) log.Error("random date error {0}", ex.Message);
					}
				
					cmdRandomData.Parameters[0].Value = randomizer;
					cmdRandomData.Parameters[1].Value = sparse_key;
					cmdRandomData.Parameters[2].Value = dense_key;
					cmdRandomData.Parameters[3].Value = sparse_signed;
					cmdRandomData.Parameters[4].Value = uniform100_dense;
					cmdRandomData.Parameters[5].Value = zipf10_float;
					cmdRandomData.Parameters[6].Value = zipf100_float;
					cmdRandomData.Parameters[7].Value = uniform100_float;
					cmdRandomData.Parameters[8].Value = double_normal;
					cmdRandomData.Parameters[9].Value = r10pct_key;				
					cmdRandomData.Parameters[10].Value = col_date;
					cmdRandomData.Parameters[11].Value = col_code;
					cmdRandomData.Parameters[12].Value = col_name;
					cmdRandomData.Parameters[13].Value = col_address;

					cmdRandomData.ExecuteNonQuery();
				}
			
				cmdRandomData.Dispose();			
				TransactionCommit();
		    
				TransactionBegin();
				ExecuteStatement(
					"update random_data set"			+
					" address='SILICON VALLEY' where "	+
					" randomizer = " + randomizer.ToString());
				TransactionCommit();
		
				/* Now generate a table with 10% of some of the fields */
				TransactionBegin();
				command = new FbCommand(
					"SELECT FIRST " + tenpct.ToString()			+
					" sparse_signed, double_normal, address"	+
					" FROM random_data"							+
					" ORDER BY randomizer", connection, transaction);
				adapter = new FbDataAdapter(command);
			
				dataset = new DataSet("RANDOM_DATA");
				adapter.Fill(dataset, "RANDOM_DATA");

				FbCommand cmdRandomTenpct = new FbCommand();

				cmdRandomTenpct.CommandText = 
					"INSERT INTO random_tenpct ("	+
					" col_key, col_signed, col_float, col_double, col_address)" +
					" VALUES (?, ?, ?, ?, ?)";
			
				cmdRandomTenpct.Connection	= connection;
				cmdRandomTenpct.Transaction	= transaction;

				cmdRandomTenpct.Parameters.Add("@col_key", FbType.Integer);
				cmdRandomTenpct.Parameters.Add("@col_signed", FbType.Integer);
				cmdRandomTenpct.Parameters.Add("@col_float", FbType.Float);
				cmdRandomTenpct.Parameters.Add("@col_double", FbType.Double);
				cmdRandomTenpct.Parameters.Add("@col_address", FbType.VarChar);

				cmdRandomTenpct.Prepare();

				rec = 1;
				foreach (DataRow row in dataset.Tables["RANDOM_DATA"].Rows)
				{	    
					cmdRandomTenpct.Parameters[0].Value = (rec == 1) ? 0 : rec;
					cmdRandomTenpct.Parameters[1].Value = Convert.ToInt64(row["sparse_signed"]);
					cmdRandomTenpct.Parameters[2].Value = Convert.ToSingle(Convert.ToDouble(row["double_normal"]) / 2.0);
					cmdRandomTenpct.Parameters[3].Value = Convert.ToDouble(row["double_normal"]);
					cmdRandomTenpct.Parameters[4].Value = Convert.ToString(row["address"]);

					cmdRandomTenpct.ExecuteNonQuery();

					rec++;
				}
				cmdRandomTenpct.Dispose();
				command.Dispose();
				adapter.Dispose();
				dataset.Dispose();
				TransactionCommit();

				TransactionBegin();
				ExecuteStatement("create index random10_ix on random_tenpct(col_key)");
				TransactionCommit();
		    
				/* Now generate a table with only 100 tuples of interesting data */
				TransactionBegin();
				CursorOpen(
					"SELECT FIRST 100"										+
					" uniform100_float, double_normal, name, address"	+
					" FROM random_data"										+
					" ORDER BY randomizer");
	
				i = 0;
				while (CursorFetch())
				{	        
					hundred_unique_float[i]		= Convert.ToSingle(Convert.ToDouble(Cursor["double_normal"]) / 2);
					hundred_unique_double[i]	= Convert.ToDouble(Cursor["double_normal"]);
					hundred_unique_name[i]		= Convert.ToString(Cursor["name"]);
					hundred_unique_address[i]	= Convert.ToString(Cursor["address"]);

					i++;
				}
				i = randNumber.Next(0, 10);
				CursorClose();
				TransactionCommit();

				col_double = hundred_unique_double[i];
				TransactionBegin();
				ExecuteStatement("update random_data"	+
					" set code = 'BENCHMARKS', name = 'THE+ASAP+BENCHMARKS+' where" +
					" double_normal = " + col_double.ToString());
				TransactionCommit();

				hundred_unique_name[i] = "THE+ASAP+BENCHMARKS+";
		    
				
				/* Now generate our testing tables */									    
		
				/* Insert data in Uniques table	*/
				TransactionBegin();
				
				FbCommand cmdUniques = new FbCommand();
		
				cmdUniques.Connection	= connection;
				cmdUniques.Transaction	= transaction;
				cmdUniques.CommandText	= 
					"insert into UNIQUES "										+
						"select sparse_key, sparse_key, sparse_signed,"			+
								"zipf10_float, double_normal, double_normal,"	+
								"col_date, code, name, address "				+
						"from random_data";

				cmdUniques.ExecuteNonQuery();				
				cmdUniques.Dispose();

				TransactionCommit();

				/* Insert data in Updates table	*/
				TransactionBegin();

				FbCommand cmdUpdates = new FbCommand();

				cmdUpdates.Connection	= connection;
				cmdUpdates.Transaction	= transaction;
				cmdUpdates.CommandText	= 
					"insert into UPDATES "										+
						"select dense_key, dense_key, sparse_signed,"			+
								"zipf10_float, double_normal, double_normal,"	+
								"col_date, code, name, address "				+
						"from random_data";

				cmdUpdates.ExecuteNonQuery();

				cmdUpdates.Dispose();

				TransactionCommit();

				/* Insert data in Hundred table	*/
				TransactionBegin();
				
				command = new FbCommand(
					"SELECT randomizer, sparse_key, dense_key, sparse_signed,"	+
					" uniform100_dense, zipf10_float, zipf100_float,"			+
					" uniform100_float, double_normal,"							+
					" col_date, code, name, address"							+
					" FROM random_data"											+
					" ORDER BY randomizer", connection, transaction);
			
				adapter = new FbDataAdapter(command);
			
				dataset = new DataSet("RANDOM_DATA");
				adapter.Fill(dataset, "RANDOM_DATA");
				
				FbCommand cmdHundred = new FbCommand();

				cmdHundred.Connection	= connection;
				cmdHundred.Transaction	= transaction;
				cmdHundred.CommandText	= 
					"INSERT INTO hundred ("								+
					" col_key, col_int, col_signed,"					+
					" col_float, col_double, col_decim,"				+
					" col_date, col_code, col_name, col_address)"		+
					" VALUES (?,?,?,?,?,?,?,?,?,?)";

				cmdHundred.Parameters.Add("@col_key", FbType.Integer);
				cmdHundred.Parameters.Add("@col_int", FbType.Integer);
				cmdHundred.Parameters.Add("@col_signed", FbType.Integer);
				cmdHundred.Parameters.Add("@col_float", FbType.Float);
				cmdHundred.Parameters.Add("@col_double", FbType.Double);
				cmdHundred.Parameters.Add("@col_decim", FbType.Decimal);
				cmdHundred.Parameters.Add("@col_date", FbType.VarChar);
				cmdHundred.Parameters.Add("@col_code", FbType.VarChar);
				cmdHundred.Parameters.Add("@col_name", FbType.VarChar);
				cmdHundred.Parameters.Add("@col_address", FbType.VarChar);

				cmdHundred.Prepare();

				hundred_key	= 0;

				foreach (DataRow row in dataset.Tables["RANDOM_DATA"].Rows)
				{			
					if (++hundred_key >= 100)
					{
						hundred_key = 0;
					}

					/* Insert into Hundred	*/
					cmdHundred.Parameters[0].Value = Convert.ToString(row["dense_key"]);
					cmdHundred.Parameters[1].Value = Convert.ToString(row["dense_key"]);
					cmdHundred.Parameters[2].Value = Convert.ToString(row["uniform100_dense"]);
					cmdHundred.Parameters[3].Value = hundred_unique_float[hundred_key];
					cmdHundred.Parameters[4].Value = hundred_unique_double[hundred_key];
					cmdHundred.Parameters[5].Value = hundred_unique_double[hundred_key];
					cmdHundred.Parameters[6].Value = Convert.ToString(row["col_date"]);
					cmdHundred.Parameters[7].Value = Convert.ToString(row["code"]);
					cmdHundred.Parameters[8].Value = hundred_unique_name[hundred_key];
					cmdHundred.Parameters[9].Value = hundred_unique_address[hundred_key];

					cmdHundred.ExecuteNonQuery();
				}
				
				cmdHundred.Dispose();

				command.Dispose();
				adapter.Dispose();
				dataset.Dispose();
				TransactionCommit();

				/* Insert data in TenPct table	*/
				TransactionBegin();
				
				FbCommand cmdTenPct = new FbCommand();

				cmdTenPct.Connection	= connection;
				cmdTenPct.Transaction	= transaction;
				cmdTenPct.CommandText	=
					"INSERT INTO tenpct "																				+
						"SELECT random_data.sparse_key, random_data.sparse_key, random_tenpct.col_signed,"				+
								"random_tenpct.col_float, random_tenpct.col_double, random_tenpct.col_double,"			+
								"random_data.col_date, random_data.code, random_data.name, random_tenpct.col_address "	+
						"FROM  random_data, random_tenpct "																+
						"WHERE random_data.r10pct_key = random_tenpct.col_key";

				cmdTenPct.ExecuteNonQuery();

				cmdTenPct.Dispose();

				TransactionCommit();

				/* Insert data in Tiny table	*/

				TransactionBegin();			
				ExecuteStatement("INSERT INTO tiny values(0)" );
				TransactionCommit();

				TransactionBegin();
				ExecuteStatement("drop table random_data");
				ExecuteStatement("drop table random_tenpct");
				TransactionCommit();
			}
			catch (Exception ex)
			{
				TransactionRollback();
				if (log != null) log.Error("load failed {0}", ex.Message);
			}
		}

		#endregion
	}
}
