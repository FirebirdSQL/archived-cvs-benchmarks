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
using System.Configuration;
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
				// Firebird dont have clustered indexes
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
			// ADO.NET interfaces dont support database creation
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


		public int createData(long dataSize)
		{
			// TODO : Change all char[] to string
	        char[] col_address				= new char[81];
	        char[] col_code					= new char[11];
			char[] col_name					= new char[21];
			char[] date_string				= new char[32];
			char[] hundred_address			= new char[81];
			char[] hundred_name				= new char[81];
			char[,] hundred_unique_address	= new char[100, 81];
			char[,] hundred_unique_code		= new char[100, 11];
			char[,] hundred_unique_name		= new char[100, 21];
			char[] name						= new char[21];
			
			float col_float;
			float hundred_float;
			float[] hundred_unique_float = new float[100];
			float uniform100_dense;
			float uniform100_float;
			float[] zipf10 = new float[10];
			float zipf10_float;
			float[] zipf100 = new float[100];
			float zipf100_float;
			
			long i;
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
			
			DateTime		tm = new DateTime();
			
			StringBuilder	sqlCommand = new StringBuilder();

			Random			randNumber = new Random();
		
		    string csv_safe_chars = "#%&()[]{};:/~@ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-=";
			/* These characters can be used without hassle in comma-separated-value files */
		    
		    int Ncsv_safe_chars;
			int Nlen;				
				    
		    Ncsv_safe_chars = csv_safe_chars.Length;
		    
		    /* For our Zipfian distributions, we'll generate values that occur
		     * most often at Zipf[0], and decay across an asymptotic curve to
		     * the value at zipf[RANKS_zipfian-1].  (If someone has a better
		     * algorithm for generating better distributions, please submit it!)
		     */
		    for (i = 0; i < 10; i++)
		    {
		        zipf10[i] = (float)randNumber.Next(-5*(HUNDREDMILLION), 5*(HUNDREDMILLION));
		    }
		    for (i = 0; i < 100; i++)
		    {
		        zipf100[i] = (float)randNumber.Next(-5*(HUNDREDMILLION), 5*(HUNDREDMILLION));
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
		        "create table random_data("				+
			        " randomizer int not null,"			+
			        " sparse_key int not null,"			+
			        " dense_key	int not null,"			+
			        " sparse_signed int not null"		+
			        " uniform100_dense int not null,"	+
			        " zipf10_float float not null,"		+
			        " zipf100_float float not null,"	+
			        " uniform100_float float not null,"	+
			        " double_normal double not null,"	+
			        " code char(10) not null,"			+
			        " name char(20) not null,"			+
			        " address varchar(800) not null)");
		    		
		    CreateTable(
		         "create table random_tenpct("			+
			         " col_key 		int not null,"		+
			         " col_float	int not null,"		+
			         " col_signed 	int not null,"		+
			         " col_double 	double not null," 	+
			         " address varchar(800) not null)");
					
			TransactionBegin();
		    for (long rec = 1; rec <= dataSize; rec++)
		    {
		        int	Dlen;
				int Drec;
		    	
		        randomizer 			= randNumber.Next(0, THOUSANDMILLION);
		        dense_key  			= (rec == 1) ? 0 : rec;       
		        sparse_key 			= dense_key * sparse_key_spread;
		        sparse_signed 		= (-5*(HUNDREDMILLION)) + 
		        					((dense_key) * sparse_signed_spread);
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
		        Dlen = 0;
		        Drec = (int)rec;
		        while (Drec > 0) 
		        {
		            col_code[Dlen++] = csv_safe_chars[Drec % Ncsv_safe_chars];
		            Drec			/= Ncsv_safe_chars;
		        }
		        col_code[Dlen++] = '_';
		        for (i = Dlen; i < 10; i++)
		        {
		            col_code[i] = csv_safe_chars[randNumber.Next(0, Ncsv_safe_chars)];
		        }
		        col_code[10] = '\0';
		        // TODO
		        // strncpy(col_name, , Dlen);
		    	for (i = Dlen; i < 20; i++)
		        {
		            col_name[i] = csv_safe_chars[randNumber.Next(0, Ncsv_safe_chars)];
		        } 
		        col_name[20] = '\0';
		    	// TODO
		        // strncpy(col_address, col_code, Dlen);		    	
		        Nlen = randNumber.Next(2, (int)(6 + (25 * (rec & 3))));
		        for (i = Dlen; i < Nlen; i++)
		        {
		            col_address[i] = csv_safe_chars[randNumber.Next(0, Ncsv_safe_chars)];
		        }
		        col_address[(Dlen > Nlen ? Dlen : Nlen)] = '\0';
		        dml(
		            "INSERT INTO random_data ("												+
		                " randomizer, sparse_key, dense_key, sparse_signed, uniform100_dense,"+
		                " zipf10_float, zipf100_float, uniform100_float, double_normal,"	+
		                " code, name, address)"												+
		            " VALUES ("																+
		                " randomizer, sparse_key, dense_key, sparse_signed, uniform100_dense,"+
		                " zipf10_float, zipf100_float, uniform100_float, double_normal,"	+
		                " col_code, col_name, col_address)");
		    } 
		    TransactionCommit();
		    
		    dml(
		        "update random_data set"				+
	                " address='SILICON VALLEY' where "	+
	                " randomizer = " + randomizer.ToString());		    
		
		    /* Now generate a table with 10% of some of the fields */
		    CursorOpen("SELECT sparse_signed, double_normal, address"	+
							" FROM random_data"							+
							" ORDER BY randomizer"						+
							" FIRST " + tenpct.ToString());		    
		
		    for (long rec = 1; rec <= tenpct; rec++)
		    {
				// Clear sqlCommand
				sqlCommand.Remove(0, sqlCommand.Length);

		        col_key = (rec == 1) ? 0 : rec;
		    	
		    	CursorFetch();
		    	col_signed	= Convert.ToInt64(Cursor["sparse_signed"]);
		    	col_double  = Convert.ToDouble(Cursor["double_normal"]);
		    	col_address = Convert.ToString(Cursor["address"]).ToCharArray();
		        col_float 	= Convert.ToSingle(col_double / 2.0);
		    	
		    	sqlCommand.AppendFormat(
		            "INSERT INTO random_tenpct ("	+
		                " col_key, col_signed, col_float, col_double, col_address)" +
		            " VALUES ({0}, {1}, {2}, {3}, '{4})",
		            col_key, col_signed, col_float, col_double, col_address);
		        
		        dml(sqlCommand.ToString());
		    }
		    CursorClose();

		    ddl("create index random10_ix on random_tenpct(col_key)");
		    
		    /* Now generate a table with only 100 tuples of interesting data */
		    CursorOpen(
		        "SELECT uniform100_float, double_normal, name, address"	+
		        " FROM random_data"										+
		        " ORDER BY randomizer"									+
		        " FIRST 100");
	
		    for (i = 0; i < 100; i++)
		    {
		    	CursorFetch();
		    	col_float	= Convert.ToSingle(Cursor["uniform100_float"]);
		    	col_double	= Convert.ToDouble(Cursor["double_normal"]);
		    	col_name	= Convert.ToString(Cursor["name"]).ToCharArray();
		    	col_address	= Convert.ToString(Cursor["address"]).ToCharArray();
		        
		        hundred_unique_float[i]	 = Convert.ToSingle(col_double / 2);
		        hundred_unique_double[i] = col_double;
		    	// TODO
		        // strncpy(hundred_unique_name[i], col_name, 20);
		        hundred_unique_name[i, 20] = '\0';
		    	// TODO
		        // strncpy(hundred_unique_address[i], col_address, 80);
		        hundred_unique_address[i,80] = '\0';
		    }
		    i = randNumber.Next(0, 10);
		    CursorClose();

		    col_double = hundred_unique_double[i];
		    dml("update random_data"	+
	                " set code = 'BENCHMARKS', name = 'THE+ASAP+BENCHMARKS+' where" +
	                "double_normal = " + col_double.ToString());

			// TODO
		    // strcpy(hundred_unique_name[i], "THE+ASAP+BENCHMARKS+");
		    
		    /* Now generate our testing tables */
		    hundred_key	= 0;
		    tenpct_key	= 0;
		    
			CursorOpen(
		        "SELECT randomizer, sparse_key, dense_key, sparse_signed,"	+
		                " uniform100_dense, zipf10_float, zipf100_float,"	+
		                " uniform100_float, double_normal,"					+
		                " code, name, address"								+
		        " FROM random_data"											+
		        " ORDER BY randomizer");

		    while (CursorFetch())
		    {
                randomizer			= Convert.ToInt64(Cursor["randomizer"]);
                sparse_key			= Convert.ToInt64(Cursor["sparse_key"]);
                dense_key			= Convert.ToInt64(Cursor["dense_key"]);
                sparse_signed       = Convert.ToInt64(Cursor["sparse_signed"]);
                uniform100_dense	= Convert.ToSingle(Cursor["uniform100_dense"]);
                zipf10_float		= Convert.ToSingle(Cursor["zipf10_float"]);
                zipf100_float       = Convert.ToSingle(Cursor["zipf100_float"]);
                uniform100_float	= Convert.ToSingle(Cursor["uniform100_float"]);
                double_normal		= Convert.ToDouble(Cursor["double_normal"]);
                col_code			= Convert.ToString(Cursor["code"]).ToCharArray();
                col_name			= Convert.ToString(Cursor["name"]).ToCharArray();
                col_address			= Convert.ToString(Cursor["address"]).ToCharArray();

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
 
				// Empty sqlCommand
				sqlCommand.Remove(0, sqlCommand.Length);

				sqlCommand.AppendFormat(
					"INSERT INTO uniques ("										+
		                    " col_key, col_int, col_signed,"					+
		                    " col_float, col_double, col_decim,"				+
		                    " col_date, col_code, col_name, col_address)"		+
		                " VALUES ({0},{1},{2},{3},{4},{5},{6},'{7}',{8},'{9}','{10}')",
							sparse_key, sparse_key, sparse_signed,
							zipf100_float, double_normal, double_normal,
							date_string, col_code, col_name, col_address);

				dml(sqlCommand.ToString());

				// Empty sqlCommand
				sqlCommand.Remove(0, sqlCommand.Length);

				sqlCommand.AppendFormat(
					"INSERT INTO updates ("									+
		                    " col_key, col_int, col_signed,"				+
		                    " col_float, col_double, col_decim,"			+
		                    " col_date, col_code, col_name, col_address)"	+
		                " VALUES ({0},{1},{2},{3},{4},{5},{6},'{7}',{8},'{9}','{10}')",
		                    dense_key, dense_key, sparse_signed,
		                    zipf10_float, double_normal, double_normal,
							date_string, col_code, col_name, col_address);
		                    
		        dml(sqlCommand.ToString());
					
				if (++hundred_key >= 100)
				{
					hundred_key = 0;
				}
		        hundred_float	= hundred_unique_float[hundred_key];
		        hundred_double	= hundred_unique_double[hundred_key];
		        // TODO
				// strncpy(hundred_name, hundred_unique_name[hundred_key], 20);
		        hundred_name[20] = '\0';
				// TODO
		        // strncpy(hundred_address, hundred_unique_address[hundred_key], 80);
		        hundred_address[80] = '\0';

				// Empty sqlCommand
				sqlCommand.Remove(0, sqlCommand.Length);

				sqlCommand.AppendFormat(
					"INSERT INTO hundred ("										+
						" col_key, col_int, col_signed,"						+
						" col_float, col_double, col_decim,"					+
						" col_date, col_code, col_name, col_address)"			+
					" VALUES ({0},{1},{2},{3},{4},{5},{6},'{7}',{8},'{9}','{10}')",
						dense_key, sparse_key, uniform100_dense,
						hundred_float, hundred_double, hundred_double,
						date_string, col_code, hundred_name, hundred_address);

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

				// TODO
				CursorOpen("SELECT col_signed, float, double, address"		+
								"FROM random_tenpct"						+
								"WHERE key = + tenpct_key.ToString()");

				CursorFetch();

				col_signed		= Convert.ToInt64(Cursor["col_signed"]);
				col_float		= Convert.ToSingle(Cursor["float"]);
				col_double		= Convert.ToDouble(Cursor["double"]);
				col_address		= Convert.ToString(Cursor["address"]).ToCharArray();

				CursorClose();

				// TODO
		        // strncpy(col_name, hundred_unique_name[hundred_key%10], 20);

				// Empty sqlCommand
				sqlCommand.Remove(0, sqlCommand.Length);

				sqlCommand.AppendFormat(
					"INSERT INTO tenpct ("								+
						" col_key, col_int, col_signed,"				+
						" col_float, col_double, col_decim,"			+
						" col_date, col_code, col_name, col_address)"	+
					" VALUES ({0},{1},{2},{3},{4},{5},{6},'{7}',{8},'{9}','{10}')",
	                    sparse_key, sparse_key, col_signed,
						col_float, col_double, col_double,
						date_string, col_code, col_name, col_address);

		        dml(sqlCommand.ToString());
		    }
			CursorClose();
		    ddl("drop table random_data");
		    ddl("drop table random_tenpct");

		    return 0;
    	}


		#endregion
	}
}
