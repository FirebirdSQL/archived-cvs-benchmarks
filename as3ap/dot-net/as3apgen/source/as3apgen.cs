//
// AS3AP Data file Generator
//
// Author: Carlos Guzmán Álvarez <carlosga@telefonica.net>
//
// Distributable under GPL license.
// You may obtain a copy of the License at http://www.gnu.org/copyleft/gpl.html
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
using System.Xml;
using System.Configuration;
using System.Globalization;

namespace AS3AP.BenchMark.Generator
{
	public class As3apGen
	{
		#region CONSTANTS

		private const int HUNDREDMILLION	= 10*10*10*10*10*10*10*10;
		private const int THOUSANDMILLION	= HUNDREDMILLION*10;

		private const int RANDOM_MIN_VALUE  = -5 * (HUNDREDMILLION);
		private const int RANDOM_MAX_VALUE  = 5 * (HUNDREDMILLION);

		private const string UPDATES_FILE_NAME	= "asap.updates";
		private const string UNIQUES_FILE_NAME	= "asap.uniques";
		private const string HUNDRED_FILE_NAME	= "asap.hundred";
		private const string TENPCT_FILE_NAME	= "asap.tenpct";
		private const string TINY_FILE_NAME		= "asap.tiny";

		#endregion

		#region FIELDS

		private string	destDir	= String.Empty;
		private long	dataSize = 0;

		#endregion

		#region MAIN

		static void Main(string[] args)
		{
			As3apGen as3apgen = 
				new As3apGen(
						ConfigurationSettings.AppSettings["DestDir"],
						Convert.ToInt32(ConfigurationSettings.AppSettings["DataSize"]));

			as3apgen.GenerateDataFiles();

			Console.WriteLine("Finished");
		}

		#endregion

		#region CONSTRUCTORS

		public As3apGen(string destDir, long dataSize)
		{
			this.destDir = destDir;
			this.dataSize = dataSize;
		}

		#endregion

        /// <summary>
        /// This method is based on OSDB project ( sourceforge.net/projects/osdb )
        /// create_data function.
        /// </summary>
		public void GenerateDataFiles()
		{
			string col_address				= String.Empty;
			string col_code					= String.Empty;
			string col_name					= String.Empty;
			string date_string				= String.Empty;
			string hundred_address			= String.Empty;
			string hundred_name				= String.Empty;
			string name						= String.Empty;

			string[] hundred_unique_address	= new string[100];
			string[] hundred_unique_code	= new string[100];
			string[] hundred_unique_name	= new string[100];
			double[] hundred_unique_double	= new double[100];
			float[]	hundred_unique_float	= new float[100];
			float[] zipf100					= new float[100];
			float[] zipf10					= new float[10];
			
			float uniform100_dense;
			float uniform100_float;			
			float zipf10_float;			
			float zipf100_float;
			
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

			int random;
			
			long[] randomSeed = new long[dataSize];

			DateTime	col_date	= new DateTime();
			Random		randNumber	= new Random();

			DsRandomData	dsRandomData	= new DsRandomData();
			DsRandomTenpct	dsRandomTenpct	= new DsRandomTenpct();

			StreamWriter dfUpdates	= null;
			StreamWriter dfUniques	= null;
			StreamWriter dfHundred	= null;
			StreamWriter dfTenpct	= null;
			StreamWriter dfTiny		= null;

			string filter	= String.Empty;
			string order	= String.Empty;

			string csv_safe_chars = "#%&()[]{};:/~@ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-=";

			int Nlen;				

			try
			{
				// Configure number format
				NumberFormatInfo numberFormat = new NumberFormatInfo();

				numberFormat.NumberDecimalSeparator = ".";
				numberFormat.NumberGroupSeparator	= String.Empty;

				// For our Zipfian distributions, we'll generate values that occur
				// most often at Zipf[0], and decay across an asymptotic curve to
				// the value at zipf[RANKS_zipfian-1].  (If someone has a better
				// algorithm for generating better distributions, please submit it!)
				for (int i = 0; i < 10; i++)
				{
					zipf10[i] = (float)randNumber.Next(RANDOM_MIN_VALUE, RANDOM_MAX_VALUE);
				}
				for (int i = 0; i < 100; i++)
				{
					zipf100[i] = (float)randNumber.Next(RANDOM_MIN_VALUE, RANDOM_MAX_VALUE);
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

				// Generate radom numbers
				for (rec = 0; rec < dataSize; rec++)
				{
					randomSeed[rec] = randNumber.Next(0, THOUSANDMILLION);
				}

				Array.Sort(randomSeed);

				// Generate RANDOM_DATA table	
				Console.WriteLine("Generating RANDOM_DATA table");
		
				dsRandomData.Tables["RANDOM_DATA"].BeginLoadData();

				for (rec = 1; rec <= dataSize; rec++)
				{
					int Drec;
		    	
					randomizer			= randomSeed[rec - 1];
					dense_key  			= (rec == 1) ? 0 : rec;       
					sparse_key 			= dense_key * sparse_key_spread;
					sparse_signed 		= RANDOM_MIN_VALUE + ((dense_key) * sparse_signed_spread);
					uniform100_dense 	= 100 + (rec % 100);
					zipf10_float 		= zipf10[randNumber.Next(0, (int)(rec % 10))];
					zipf100_float 		= zipf100[randNumber.Next(0, (int)(rec % 100))];
					uniform100_float 	= 100 + (float)((rec % 100));
					double_normal 		= (double)randNumber.Next(-(THOUSANDMILLION), (THOUSANDMILLION));

					// To ensure uniqueness, we'll start by generating the record number
					// in base (Ncsv_safe_chars), followed by "_". We'll then fill out
					// the field with additional randomly selected characters. (By writing
					// the digits backwards, we should help to keep the data disorderly :)
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
					for (int i = col_code.Length; i < 10; i++)
					{
						col_code += csv_safe_chars[randNumber.Next(0, csv_safe_chars.Length)];
					}
					col_name = col_code;
					for (int i = col_code.Length; i < 20; i++)
					{
						col_name += csv_safe_chars[randNumber.Next(0, csv_safe_chars.Length)];
					} 
					col_address = col_code;
					Nlen = randNumber.Next(2, (int)(6 + (25 * (rec & 3))));
					for (int i = col_code.Length; i < Nlen; i++)
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
						date_random = dense_key % 36835;
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
					catch (Exception)
					{
					}
					
					DataRow newRow = dsRandomData.Tables["RANDOM_DATA"].NewRow();
				
					newRow["RANDOMIZER"]		= randomizer;
					newRow["SPARSE_KEY"]		= sparse_key;
					newRow["DENSE_KEY"]			= dense_key;
					newRow["SPARSE_SIGNED"]		= sparse_signed;
					newRow["UNIFORM100_DENSE"]	= uniform100_dense;
					newRow["ZIPF10_FLOAT"]		= zipf10_float;
					newRow["ZIPF100_FLOAT"]		= zipf100_float;
					newRow["UNIFORM100_FLOAT"]	= uniform100_float;
					newRow["DOUBLE_NORMAL"]		= double_normal;
					newRow["R10PCT_KEY"]		= r10pct_key;
					newRow["COL_DATE"]			= col_date;
					newRow["CODE"]				= col_code;
					newRow["NAME"]				= col_name;
					newRow["ADDRESS"]			= col_address;

					dsRandomData.Tables["RANDOM_DATA"].Rows.Add(newRow);					

					if (rec % 100000 == 0)
					{
						Console.WriteLine("Record number: {0}", rec);
					}
				}

				dsRandomData.Tables["RANDOM_DATA"].EndLoadData();

				// Free radomSeed memory
				Array.Clear(randomSeed, 0, randomSeed.Length);
				randomSeed = null;
				
				// Select all rows that matches randomizer value
				Console.WriteLine("Select all rows that matches randomizer value and setting ADDRESS = 'SILICON VALLEY'");

				filter = "RANDOMIZER = " + randomizer.ToString();
				DataRow[] foundRows	= dsRandomData.Tables["RANDOM_DATA"].Select(filter);

				foreach (DataRow row in foundRows)
				{
					row["ADDRESS"] = "SILICON VALLEY";
				}

				Array.Clear(foundRows, 0, foundRows.Length);
				foundRows = null;
				
				// Now generate a table with 10% of some of the fields
				Console.WriteLine("Now generate a table with 10% of some of the fields");

				dsRandomTenpct.Tables["RANDOM_TENPCT"].BeginLoadData();
								
				random	= 0;
				rec		= 1;
				foreach (DataRow row in dsRandomData.Tables["RANDOM_DATA"].Rows)
				{
					DataRow newRow = dsRandomTenpct.Tables["RANDOM_TENPCT"].NewRow();

					newRow["COL_KEY"]		= (rec == 1) ? 0 : rec;
					newRow["COL_SIGNED"]	= row["SPARSE_SIGNED"];
					newRow["COL_FLOAT"]		= (float)(Convert.ToSingle(row["DOUBLE_NORMAL"]) / 2);
					newRow["COL_DOUBLE"]	= row["DOUBLE_NORMAL"];
					newRow["COL_ADDRESS"]	= row["ADDRESS"];

					dsRandomTenpct.Tables["RANDOM_TENPCT"].Rows.Add(newRow);

					rec++;
					
					if (random < 100)
					{
						// Now generate a table with only 100 tuples of interesting data
						//uniform100_float, double_normal, name, address
						hundred_unique_float[random]	= (float)newRow["COL_FLOAT"];
						hundred_unique_double[random]	= Convert.ToDouble(row["DOUBLE_NORMAL"]);
						hundred_unique_name[random]		= Convert.ToString(row["NAME"]);
						hundred_unique_address[random]	= Convert.ToString(row["ADDRESS"]);

						random++;
					}

					if (rec > tenpct)
					{
						break;
					}
				}
								
				dsRandomTenpct.Tables["RANDOM_TENPCT"].EndLoadData();

				// Update CODE and NAME fields of RANDOM_DATA
				// in rows where DOUBLE_NORMAL = col_double
				random = randNumber.Next(0, 10);

				col_double = hundred_unique_double[random];
								
				filter		= "DOUBLE_NORMAL = " + col_double.ToString();
				foundRows	= dsRandomData.Tables["RANDOM_DATA"].Select(filter);
				
				foreach (DataRow row in foundRows)
				{
					row["CODE"] = "BENCHMARKS";
					row["NAME"] = "THE+ASAP+BENCHMARKS+";
				}

				Array.Clear(foundRows, 0, foundRows.Length);
				foundRows = null;

				// Generate data Files
				Console.WriteLine("Generate data Files");
				hundred_key	= 0;

				dfUniques	= new StreamWriter(new FileStream(destDir + UNIQUES_FILE_NAME, FileMode.Create, FileAccess.Write, FileShare.None));
				dfUpdates	= new StreamWriter(new FileStream(destDir + UPDATES_FILE_NAME, FileMode.Create, FileAccess.Write, FileShare.None));
				dfHundred	= new StreamWriter(new FileStream(destDir + HUNDRED_FILE_NAME, FileMode.Create, FileAccess.Write, FileShare.None));
				dfTenpct	= new StreamWriter(new FileStream(destDir + TENPCT_FILE_NAME, FileMode.Create, FileAccess.Write, FileShare.None));

				foreach (DataRow row in dsRandomData.Tables["RANDOM_DATA"].Rows)
				{
					// Unique File
					dfUniques.WriteLine("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}",
						row["SPARSE_KEY"],
						row["SPARSE_KEY"],
						row["SPARSE_SIGNED"],
						((float)row["ZIPF100_FLOAT"]).ToString(numberFormat),
						((double)row["DOUBLE_NORMAL"]).ToString(numberFormat),
						((double)row["DOUBLE_NORMAL"]).ToString(numberFormat),
						row["COL_DATE"],
						row["CODE"],
						row["NAME"],
						row["ADDRESS"]);

					// Updates File
					dfUpdates.WriteLine("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}",
						row["DENSE_KEY"],
						row["DENSE_KEY"],
						row["SPARSE_SIGNED"],
						((float)row["ZIPF10_FLOAT"]).ToString(numberFormat),
						((double)row["DOUBLE_NORMAL"]).ToString(numberFormat),
						((double)row["DOUBLE_NORMAL"]).ToString(numberFormat),
						row["COL_DATE"],
						row["CODE"],
						row["NAME"],
						row["ADDRESS"]);
					
					// Hundred file
					if (++hundred_key >= 100)
					{
						hundred_key = 0;
					}

					// Insert into Hundred
					dfHundred.WriteLine("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}",
						row["DENSE_KEY"],
						row["SPARSE_KEY"],
						row["UNIFORM100_DENSE"],
						hundred_unique_float[hundred_key].ToString(numberFormat),
						hundred_unique_double[hundred_key].ToString(numberFormat),
						hundred_unique_double[hundred_key].ToString(numberFormat),
						row["COL_DATE"],
						row["CODE"],
						hundred_unique_name[hundred_key],
						hundred_unique_address[hundred_key]);

					// Insert into tenpct
					filter		= "COL_KEY = " + row["R10PCT_KEY"].ToString();
					foundRows	= dsRandomTenpct.Tables["RANDOM_TENPCT"].Select(filter);
					
					if (foundRows.Length != 0)
					{
						DataRow tenpctRow = foundRows[0];

						dfTenpct.WriteLine(
							"{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}",
							row["SPARSE_KEY"],
							row["SPARSE_KEY"],
							tenpctRow["COL_SIGNED"],
							((float)tenpctRow["COL_FLOAT"]).ToString(numberFormat),
							((double)tenpctRow["COL_DOUBLE"]).ToString(numberFormat),
							((double)tenpctRow["COL_DOUBLE"]).ToString(numberFormat),
							row["COL_DATE"],
							row["CODE"],
							row["NAME"],
							row["ADDRESS"]);								
					}

					Array.Clear(foundRows, 0, foundRows.Length);
					foundRows = null;
				}

				// Close files
				dfUpdates.Close();
				dfUpdates = null;
				
				dfUniques.Close();
				dfUniques = null;
				
				dfHundred.Close();
				dfHundred = null;
				
				dfTenpct.Close();
				dfTenpct = null;

				// Finally create TINY file
				dfTiny = new StreamWriter(new FileStream(destDir + TINY_FILE_NAME, FileMode.Create, FileAccess.Write, FileShare.None));

				dfTiny.Write("0");

				dfTiny.Close();
				dfTiny = null;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
				
				// Close files
				if (dfUpdates != null)
				{
					dfUpdates.Close();
					dfUpdates = null;
				}

				if (dfUniques != null)
				{
					dfUniques.Close();
					dfUniques = null;
				}

				if (dfHundred != null)
				{
					dfHundred.Close();
					dfHundred = null;
				}

				if (dfTenpct != null)
				{
					dfTenpct.Close();
					dfTenpct = null;
				}

				if (dfTiny != null)
				{
					dfTiny.Close();
					dfTiny = null;
				}
			}
			finally
			{
				dsRandomData.Dispose();
				dsRandomTenpct.Dispose();
			}
		}
	}
}
