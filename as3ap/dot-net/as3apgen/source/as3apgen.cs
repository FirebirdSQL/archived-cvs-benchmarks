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
// Portions of this file are based on OSDB project ( sourceforge.net/projects/osdb )
//

using System;
using System.IO;
using System.Data;
using System.Xml;
using System.Configuration;
using System.Globalization;
using System.Text;

namespace AS3AP.BenchMark.Generator
{
	public class As3apGen
	{
		#region Constants

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

		#region Fields

		private string	destDir	= String.Empty;
		private long	dataSize = 0;

		#endregion

		#region Main

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

		#region Constructors

		public As3apGen(string destDir, long dataSize)
		{
			this.destDir = destDir;
			this.dataSize = dataSize;
		}

		#endregion

		#region Methods

        /// <summary>
        /// This method is based on OSDB project ( sourceforge.net/projects/osdb )
        /// create_data function.
        /// </summary>
		public void GenerateDataFiles()
		{
			StringBuilder col_address	= new StringBuilder();
			StringBuilder col_code		= new StringBuilder();
			StringBuilder col_name		= new StringBuilder();
			string date_string			= String.Empty;
			string hundred_address		= String.Empty;
			string hundred_name			= String.Empty;
			string name					= String.Empty;

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
			long tenpct_key;
			long randomizer;
			long r10pct_key;
			long sparse_key;
			long sparse_signed;
			long sparse_key_spread;
			long sparse_signed_spread;
			long tenpct;
		        
			double col_double;
			double double_normal;			

			int random;
			int nLen;
			
			long[] randomSeed = new long[dataSize];

			DateTime		col_date		= new DateTime();
			Random			randNumber		= new Random();

			DsRandomData	dsRandomData	= new DsRandomData();

			StreamWriter dfUpdates	= null;
			StreamWriter dfUniques	= null;
			StreamWriter dfHundred	= null;
			StreamWriter dfTenpct	= null;
			StreamWriter dfTiny		= null;

			string filter	= String.Empty;
			string order	= String.Empty;

			string csv_safe_chars = "#%&()[]{};:/~@ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-=";

			randomizer = 0;
			r10pct_key = 0;
			
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

				int dRec = 0;

				for (rec = 1; rec <= dataSize; rec++)
				{
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
					// in base (csv_safe_chars), followed by "_". We'll then fill out
					// the field with additional randomly selected characters. (By writing
					// the digits backwards, we should help to keep the data disorderly :)
					dRec				= (int)rec;
					col_code.Length		= 0;
					col_name.Length		= 0;
					col_address.Length	= 0;

					// Generate col_code value
					while (dRec > 0) 
					{
						col_code.Append(csv_safe_chars[dRec % csv_safe_chars.Length]);
						dRec /= csv_safe_chars.Length;
					}
					col_code.Append('_');
					for (int i = col_code.Length; i < 10; i++)
					{
						col_code.Append(csv_safe_chars[randNumber.Next(0, csv_safe_chars.Length)]);
					}

					// Generate col_name value
					col_name.Append(col_code.ToString());
					for (int i = col_code.Length; i < 20; i++)
					{
						col_name.Append(csv_safe_chars[randNumber.Next(0, csv_safe_chars.Length)]);
					} 

					// Generate col_address value
					col_address.Append(col_code.ToString());
					nLen = randNumber.Next(2, (int)(6 + (25 * (rec & 3))));
					for (int i = col_code.Length; i < nLen; i++)
					{
						col_address.Append(csv_safe_chars[randNumber.Next(0, csv_safe_chars.Length)]);
					}

					if (col_address.Length == 0)
					{
						Console.WriteLine("ba");
					}

					// Update r10pct_key value
					if (++r10pct_key > tenpct)
					{
						r10pct_key = 0;
					} 
					else if (r10pct_key == 1) 
					{
						r10pct_key++;
					} 

					// Generate col_date value
					try
					{
						col_date	= new DateTime(1900, 1, 1);
						date_random = dense_key % 36835;
						col_date	= col_date.AddYears((int)(date_random / 365));
						date_random = (date_random % 365) + 1;

						if (date_random <= 31) 
						{
							col_date = col_date.AddMonths(0);
							col_date = col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 28) 
						{
							col_date = col_date.AddMonths(1);
							col_date = col_date.AddDays(date_random);
						} 
						else if ((date_random -= 28) <= 31) 
						{
							col_date = col_date.AddMonths(2);
							col_date = col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 30) 
						{
							col_date = col_date.AddMonths(3);
							col_date = col_date.AddDays(date_random);
						} 
						else if ((date_random -= 30) <= 31) 
						{
							col_date = col_date.AddMonths(4);
							col_date = col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 30) 
						{
							col_date = col_date.AddMonths(5);
							col_date = col_date.AddDays(date_random);
						} 
						else if ((date_random -= 30) <= 31) 
						{
							col_date = col_date.AddMonths(6);
							col_date = col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 31) 
						{
							col_date = col_date.AddMonths(7);
							col_date = col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 30) 
						{
							col_date = col_date.AddMonths(8);
							col_date = col_date.AddDays(date_random);
						} 
						else if ((date_random -= 30) <= 31) 
						{
							col_date = col_date.AddMonths(9);
							col_date = col_date.AddDays(date_random);
						} 
						else if ((date_random -= 31) <= 30) 
						{
							col_date = col_date.AddMonths(10);
							col_date = col_date.AddDays(date_random);
						} 
						else 
						{
							col_date = col_date.AddMonths(11);
							col_date = col_date.AddDays(date_random);
						}
					}
					catch (Exception)
					{
					}
					
					// Insert new row
					DataRow newRow = dsRandomData.Tables["RANDOM_DATA"].NewRow();
				
					newRow["RANDOMIZER"]		= randomizer;
#warning is this change really correct ??
//					newRow["SPARSE_KEY"]		= sparse_key;
//					newRow["DENSE_KEY"]			= dense_key;
					newRow["SPARSE_KEY"]		= rec;
					newRow["DENSE_KEY"]			= rec;
					newRow["SPARSE_SIGNED"]		= sparse_signed;
					newRow["UNIFORM100_DENSE"]	= uniform100_dense;
					newRow["ZIPF10_FLOAT"]		= zipf10_float;
					newRow["ZIPF100_FLOAT"]		= zipf100_float;
					newRow["UNIFORM100_FLOAT"]	= uniform100_float;
					newRow["DOUBLE_NORMAL"]		= double_normal;
					newRow["R10PCT_KEY"]		= r10pct_key;
					newRow["COL_DATE"]			= col_date.ToString("dd/MM/yyyy");
					newRow["COL_CODE"]			= col_code.ToString();
					newRow["COL_NAME"]			= col_name.ToString();
					newRow["COL_ADDRESS"]		= col_address.ToString();

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

				foundRows[0]["COL_ADDRESS"] = "SILICON VALLEY";

				Array.Clear(foundRows, 0, foundRows.Length);
				foundRows = null;
				
				// Now generate a table with 10% of some of the fields
				Console.WriteLine("Now generate a table with 10% of some of the fields");

				dsRandomData.Tables["RANDOM_TENPCT"].BeginLoadData();
								
				dsRandomData.Tables["RANDOM_DATA"].DefaultView.Sort = "randomizer";

				rec		= 1;
				dRec	= 0;
				foreach (DataRow row in dsRandomData.Tables["RANDOM_DATA"].Rows)
				{
					DataRow newRow = dsRandomData.Tables["RANDOM_TENPCT"].NewRow();

					newRow["COL_KEY"]		= (rec == 1) ? 0 : rec;
					newRow["COL_SIGNED"]	= row["SPARSE_SIGNED"];
					newRow["COL_FLOAT"]		= (float)(Convert.ToSingle(row["DOUBLE_NORMAL"]) / 2);
					newRow["COL_DOUBLE"]	= row["DOUBLE_NORMAL"];
					
					dsRandomData.Tables["RANDOM_TENPCT"].Rows.Add(newRow);
					
					if (dRec < 100)
					{
						// Now generate a table with only 100 tuples of interesting data
						// uniform100_float, double_normal, name, address
						hundred_unique_float[dRec]	= (float)newRow["COL_FLOAT"];
						hundred_unique_double[dRec]	= Convert.ToDouble(row["DOUBLE_NORMAL"]);
						hundred_unique_name[dRec]	= Convert.ToString(row["COL_NAME"]);
						hundred_unique_address[dRec]= Convert.ToString(row["COL_ADDRESS"]);

						dRec++;
					}

					if (++rec > tenpct)
					{
						break;
					}
				}

				random	= randNumber.Next(0, 100);

				hundred_unique_address[random] = "SILICON VALLEY";

				dsRandomData.Tables["RANDOM_DATA"].DefaultView.Sort = "";
								
				dsRandomData.Tables["RANDOM_TENPCT"].EndLoadData();

				// Update CODE and NAME fields of RANDOM_DATA
				// in rows where DOUBLE_NORMAL = col_double
				random = randNumber.Next(0, 10);

				col_double = hundred_unique_double[random];
								
				filter		= "DOUBLE_NORMAL = " + col_double.ToString();
				foundRows	= dsRandomData.Tables["RANDOM_DATA"].Select(filter);
				
				foundRows[0]["COL_CODE"] = "BENCHMARKS";

				Array.Clear(foundRows, 0, foundRows.Length);
				foundRows = null;

				// Generate data Files
				Console.WriteLine("Generate data Files");
				
				hundred_key	= 0;
				tenpct_key	= 0;

				dfUniques	= this.createStream(destDir + UNIQUES_FILE_NAME);
				dfUpdates	= this.createStream(destDir + UPDATES_FILE_NAME);
				dfHundred	= this.createStream(destDir + HUNDRED_FILE_NAME);
				dfTenpct	= this.createStream(destDir + TENPCT_FILE_NAME);

				rec		= 0;
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
						row["COL_CODE"],
						row["COL_NAME"],
						row["COL_ADDRESS"]);

					// Updates File
					dfUpdates.WriteLine("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}",
						row["DENSE_KEY"],
						row["DENSE_KEY"],
						row["SPARSE_SIGNED"],
						((float)row["ZIPF10_FLOAT"]).ToString(numberFormat),
						((double)row["DOUBLE_NORMAL"]).ToString(numberFormat),
						((double)row["DOUBLE_NORMAL"]).ToString(numberFormat),
						row["COL_DATE"],
						row["COL_CODE"],
						row["COL_NAME"],
						row["COL_ADDRESS"]);
					
					// Hundred file

					if (++hundred_key >= 100)
					{
						hundred_key = 0;
					}
					
					dfHundred.WriteLine("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}",
						row["DENSE_KEY"],
						row["SPARSE_KEY"],
						row["UNIFORM100_DENSE"],
						hundred_unique_float[hundred_key].ToString(numberFormat),
						hundred_unique_double[hundred_key].ToString(numberFormat),
						hundred_unique_double[hundred_key].ToString(numberFormat),
						row["COL_DATE"],
						row["COL_CODE"],
						hundred_unique_name[hundred_key],
						hundred_unique_address[hundred_key]);

					filter		= "COL_KEY = " + row["R10PCT_KEY"].ToString();
					foundRows	= dsRandomData.Tables["RANDOM_TENPCT"].Select(filter);
					
					if (foundRows.Length != 0)
					{
						col_name.Length = 0;

						// Generate a 10% rows with 'THE+ASAP+BENCHMARKS+'
						// needed for sel_10pct_ncl test
						if (tenpct_key == 0)
						{
							col_name.Append("THE+ASAP+BENCHMARKS+");
						}
						else
						{
							col_name.Append(row["COL_NAME"].ToString());
						}

						if (++tenpct_key >= 10)
						{
							tenpct_key = 0;
						}

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
							row["COL_CODE"],
							col_name.ToString(),
							row["COL_ADDRESS"]);
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
			}
		}

		#endregion

		#region Private Methods

		private StreamWriter createStream(string fileName)
		{
			return new StreamWriter(new BufferedStream(new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None)));
		}

		#endregion
	}
}
