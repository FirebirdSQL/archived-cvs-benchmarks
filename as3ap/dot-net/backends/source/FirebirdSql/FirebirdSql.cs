//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
//
// Ported from OSDB project at http://osdb.sourceforge.net
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
using System.Configuration;
using System.Globalization;

using AS3AP.LogData;

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
			
			log = new Logger(GetType(), "as3ap.log", Mode.OVERWRITE);

			// Use point as decimal separator
			numberFormat.CurrencyDecimalSeparator = ".";
		}

		#endregion

		#region METHODS

		private void getConfiguration()
		{
			connectionString	= ConfigurationSettings.AppSettings["ConnectionString"];
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
			StringBuilder commandText = new StringBuilder();

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
			StringBuilder commandText = new StringBuilder();

			commandText.AppendFormat("create unique index {0} on {1} ({2})",
									iName, tName, fields);

			try
			{
				/* This is not needed, Firebird creates a new unique index
				 * when a table has a primary key defined.
				 */
				// ddl(commandText.ToString());
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
			StringBuilder commandText = new StringBuilder();

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
			StringBuilder commandText = new StringBuilder();

			commandText.AppendFormat("create index {0} on {1} ({2})",
									iName, tName, fields);

			try
			{
				// Firebird doesn't have clustered indexes
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
				log.Error("connection error");
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
			FbCommand command = null;

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

		public int CreateData(long dataSize)
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

			float  col_float;
			float  hundred_float;
			float[] hundred_unique_float = new float[100];
			float  uniform100_dense;
			float  uniform100_float;
			float[] zipf10 = new float[10];
			float  zipf10_float;
			float[] zipf100 = new float[100];
			float zipf100_float;
			
			long i;
			long rec;
			long col_key;
			long col_signed;
			long date_random;
			long dense_key;
			long hundred_key;			
			long randomizer = 0;
			long sparse_key;
			long sparse_signed;
			long sparse_key_spread;
			long sparse_signed_spread;
			long tenpct;
			long tenpct_key;
		        
			double col_double;
			double double_normal;
			double hundred_double;
			double[] hundred_unique_double = new double[100];
			
			DateTime		tm			= new DateTime();			
			StringBuilder	sqlCommand	= new StringBuilder();
			Random			randNumber	= new Random();
		
			FbDataAdapter	adapter = null;
			FbCommand		command = null;
			DataSet			dataset = null;

			/* These characters can be used without hassle in comma-separated-value files */
			// string csv_safe_chars = "#%&()[]{};:/~@ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-=";
			string csv_safe_chars = "#%&()[]{}:_/~@ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-=";
		    
			int Nlen;				

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
		    				
			try
			{
				CreateTable(
					"create table random_data("					+
					" randomizer int not null,"					+
					" sparse_key int not null,"					+
					" dense_key	int not null,"					+
					" sparse_signed int not null,"				+
					" uniform100_dense int not null,"			+
					" zipf10_float float not null,"				+
					" zipf100_float float not null,"			+
					" uniform100_float float not null,"			+
					" double_normal double precision not null,"	+
					" code char(10) not null,"					+
					" name char(20) not null,"					+
					" address varchar(800) not null)");
		    		
				CreateTable(
					"create table random_tenpct("					+
					" col_key 		int not null,"					+
					" col_float		int not null,"					+
					" col_signed 	int not null,"					+
					" col_double 	double precision not null," 	+
					" col_address	varchar(800) not null)");
			}
			catch(Exception)
			{
			}
			
			TransactionBegin();
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
				Drec = (int)rec;
				col_code = String.Empty;
				col_name = String.Empty;
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
				
				sqlCommand = new StringBuilder();
				sqlCommand.AppendFormat(
					numberFormat,
					"INSERT INTO random_data ("												+
						" randomizer, sparse_key, dense_key, sparse_signed, uniform100_dense,"+
						" zipf10_float, zipf100_float, uniform100_float, double_normal,"	+
						" code, name, address)"												+
					" VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},'{9}','{10}','{11}')",
						randomizer, sparse_key, dense_key, sparse_signed, uniform100_dense,
						zipf10_float, zipf100_float, uniform100_float, double_normal,
						col_code, col_name, col_address);				

				dml(sqlCommand.ToString());
			}
			TransactionCommit();
		    
			TransactionBegin();
			dml(
				"update random_data set"				+
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

			rec = 1;
			foreach (DataRow row in dataset.Tables["RANDOM_DATA"].Rows)
			{
				sqlCommand = new StringBuilder();

				col_key		= (rec == 1) ? 0 : rec;
				col_signed	= Convert.ToInt64(row["sparse_signed"]);
				col_double  = Convert.ToDouble(row["double_normal"]);
				col_address = Convert.ToString(row["address"]);
				col_float 	= Convert.ToSingle(col_double / 2.0);
		    
				sqlCommand.AppendFormat(
					numberFormat,
					"INSERT INTO random_tenpct ("	+
						" col_key, col_signed, col_float, col_double, col_address)" +
					" VALUES ({0}, {1}, {2}, {3}, '{4}')",
						col_key, col_signed, col_float, col_double, col_address);

				dml(sqlCommand.ToString());

				rec++;
			}
			TransactionCommit();
			command.Dispose();
			adapter.Dispose();
			dataset.Dispose();

			ddl("create index random10_ix on random_tenpct(col_key)");
		    
			/* Now generate a table with only 100 tuples of interesting data */
			TransactionBegin();
			CursorOpen(
				"SELECT FIRST 100"									+
					" uniform100_float, double_normal, name, address"	+
				" FROM random_data"									+
					" ORDER BY randomizer");
	
			i = 0;
			while (CursorFetch())
			{
				col_float	= Convert.ToSingle(Cursor["uniform100_float"]);
				col_double	= Convert.ToDouble(Cursor["double_normal"]);
				col_name	= Convert.ToString(Cursor["name"]);
				col_address	= Convert.ToString(Cursor["address"]);
		        
				hundred_unique_float[i]		= Convert.ToSingle(col_double / 2);
				hundred_unique_double[i]	= col_double;
				hundred_unique_name[i]		= col_name;
				hundred_unique_address[i]	= col_address;

				i++;
			}
			i = randNumber.Next(0, 10);
			CursorClose();
			TransactionCommit();

			col_double = hundred_unique_double[i];
			TransactionBegin();
			dml("update random_data"	+
				" set code = 'BENCHMARKS', name = 'THE+ASAP+BENCHMARKS+' where" +
				" double_normal = " + col_double.ToString());
			TransactionCommit();

			hundred_unique_name[i] = "THE+ASAP+BENCHMARKS+";
		    
			/* Now generate our testing tables */
			hundred_key	= 0;
			tenpct_key	= 0;
		    
			TransactionBegin();

			command = new FbCommand(
				"SELECT randomizer, sparse_key, dense_key, sparse_signed,"	+
				" uniform100_dense, zipf10_float, zipf100_float,"			+
				" uniform100_float, double_normal,"							+
				" code, name, address"										+
				" FROM random_data"											+
				" ORDER BY randomizer", connection, transaction);
			
			adapter = new FbDataAdapter(command);
			
			dataset = new DataSet("RANDOM_DATA");
			adapter.Fill(dataset, "RANDOM_DATA");

			foreach (DataRow row in dataset.Tables["RANDOM_DATA"].Rows)
			{
				randomizer			= Convert.ToInt64(row["randomizer"]);
				sparse_key			= Convert.ToInt64(row["sparse_key"]);
				dense_key			= Convert.ToInt64(row["dense_key"]);
				sparse_signed       = Convert.ToInt64(row["sparse_signed"]);
				uniform100_dense	= Convert.ToSingle(row["uniform100_dense"]);
				zipf10_float		= Convert.ToSingle(row["zipf10_float"]);
				zipf100_float       = Convert.ToSingle(row["zipf100_float"]);
				uniform100_float	= Convert.ToSingle(row["uniform100_float"]);
				double_normal		= Convert.ToDouble(row["double_normal"]);
				col_code			= Convert.ToString(row["code"]);
				col_name			= Convert.ToString(row["name"]);
				col_address			= Convert.ToString(row["address"]);

				try
				{
					/* roughly 36,835 days from 1/1/1900-12/1/2000 */
					date_random = dense_key % 36835;
					/* ignore leap year considerations */
					tm.AddYears((int)(date_random / 365));
					date_random = (date_random % 365) + 1;
					if (date_random <= 31) 
					{
						tm.AddMonths(0);
						tm.AddDays(date_random);
					} 
					else if ((date_random -= 31) <= 28) 
					{
						tm.AddMonths(1);
						tm.AddDays(date_random);
					} 
					else if ((date_random -= 28) <= 31) 
					{
						tm.AddMonths(2);
						tm.AddDays(date_random);
					} 
					else if ((date_random -= 31) <= 30) 
					{
						tm.AddMonths(3);
						tm.AddDays(date_random);
					} 
					else if ((date_random -= 30) <= 31) 
					{
						tm.AddMonths(4);
						tm.AddDays(date_random);
					} 
					else if ((date_random -= 31) <= 30) 
					{
						tm.AddMonths(5);
						tm.AddDays(date_random);
					} 
					else if ((date_random -= 30) <= 31) 
					{
						tm.AddMonths(6);
						tm.AddDays(date_random);
					} 
					else if ((date_random -= 31) <= 31) 
					{
						tm.AddMonths(7);
						tm.AddDays(date_random);
					} 
					else if ((date_random -= 31) <= 30) 
					{
						tm.AddMonths(8);
						tm.AddDays(date_random);
					} 
					else if ((date_random -= 30) <= 31) 
					{
						tm.AddMonths(9);
						tm.AddDays(date_random);
					} 
					else if ((date_random -= 31) <= 30) 
					{
						tm.AddMonths(10);
						tm.AddDays(date_random);
					} 
					else 
					{
						tm.AddMonths(11);
						tm.AddDays(date_random);
					}
				}
				catch (Exception)
				{
					log.Error("random date error");
				}

				sqlCommand = new StringBuilder();
				sqlCommand.AppendFormat(
					numberFormat,
					"INSERT INTO uniques ("									+
						" col_key, col_int, col_signed,"					+
						" col_float, col_double, col_decim,"				+
						" col_date, col_code, col_name, col_address)"		+
					" VALUES ({0},{1},{2},{3},{4},{5},'{6}','{7}','{8}','{9}')",
						sparse_key, sparse_key, sparse_signed,
						zipf100_float, double_normal, double_normal,
						tm, col_code, col_name, col_address);

				dml(sqlCommand.ToString());

				sqlCommand = new StringBuilder();
				sqlCommand.AppendFormat(
					numberFormat,
					"INSERT INTO updates ("								+
						" col_key, col_int, col_signed,"				+
						" col_float, col_double, col_decim,"			+
						" col_date, col_code, col_name, col_address)"	+
					" VALUES ({0},{1},{2},{3},{4},{5},'{6}','{7}','{8}','{9}')",
						dense_key, dense_key, sparse_signed,
						zipf10_float, double_normal, double_normal,
						tm, col_code, col_name, col_address);
		            
				dml(sqlCommand.ToString());
			
				if (++hundred_key >= 100)
				{
					hundred_key = 0;
				}
				hundred_float	= hundred_unique_float[hundred_key];
				hundred_double	= hundred_unique_double[hundred_key];
				hundred_name	= hundred_unique_name[hundred_key];
				hundred_address	= hundred_unique_address[hundred_key];

				sqlCommand = new StringBuilder();
				sqlCommand.AppendFormat(
					numberFormat,
					"INSERT INTO hundred ("										+
						" col_key, col_int, col_signed,"						+
						" col_float, col_double, col_decim,"					+
						" col_date, col_code, col_name, col_address)"			+
					" VALUES ({0},{1},{2},{3},{4},{5},'{6}','{7}','{8}','{9}')",
						dense_key, sparse_key, uniform100_dense,
						hundred_float, hundred_double, hundred_double,
						tm, col_code, hundred_name, hundred_address);

				dml(sqlCommand.ToString());

				if (++tenpct_key > tenpct) 
				{
					tenpct_key = 0;
				} 
				else 
				{
					if (tenpct_key == 1) 
					{
						tenpct_key = 2;
					}
				}

				CursorOpen(
					"SELECT col_signed, col_float, col_double, col_address"	+
					" FROM random_tenpct"									+
					" WHERE col_key = " + tenpct_key.ToString());

				if (CursorFetch())
				{
					col_signed		= Convert.ToInt64(Cursor["col_signed"]);
					col_float		= Convert.ToSingle(Cursor["col_float"]);
					col_double		= Convert.ToDouble(Cursor["col_double"]);
					col_address		= Convert.ToString(Cursor["col_address"]);

					CursorClose();

					col_name = hundred_unique_name[hundred_key % 10];

					sqlCommand = new StringBuilder();
					sqlCommand.AppendFormat(
						numberFormat,
						"INSERT INTO tenpct ("							+
							" col_key, col_int, col_signed,"				+
							" col_float, col_double, col_decim,"			+
							" col_date, col_code, col_name, col_address)"	+
						" VALUES ({0},{1},{2},{3},{4},{5},'{6}','{7}','{8}','{9}')",
							sparse_key, sparse_key, col_signed,
							col_float, col_double, col_double,
							tm, col_code, col_name, col_address);

					dml(sqlCommand.ToString());
				}
				else
				{
					CursorClose();
				}
			}
			command.Dispose();
			adapter.Dispose();
			dataset.Dispose();
			
			dml("INSERT INTO tiny values(0)" );

			TransactionCommit();

			ddl("drop table random_data");
			ddl("drop table random_tenpct");

			return 0;
		}

		#endregion
	}
}
