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
using System.Runtime.Serialization.Formatters.Soap;

namespace AS3AP.BenchMark
{
	/// <summary>
	/// Descripción breve de Configuration.
	/// </summary>
	[Serializable]
	public class BenchMarkConfiguration
	{
		#region FIELDS

		private int		userNumber			= 0;
		private long	dataSize			= 10000;
		private string	runSequence			= "SQL87;singleuser;SQL92;singleuser";
		private bool	runCreate			= true;
		private string	dataPath			= String.Empty;
		private bool	useIndexes			= true;

		private string	connectionString	= "AS3AP.FDB;User=SYSDBA;Password=masterkey;Server=localhost;Port=3050;Dialect=3;Charset=NONE;Role=;Connection Lifetime=15;Pooling=true";

		private string	providerAssembly	= "FirebirdSql.Data.Firebird";
		private bool	supportsClusteredIndexes = false;
		private bool	supportsHashIndexes = false;
		private string	connectionClass		= "FirebirdSql.Data.Firebird.FbConnection";
		private string	commandClass		= "FirebirdSql.Data.Firebird.FbCommand";
		private string	dataAdapterClass	= "FirebirdSql.Data.Firebird.FbDataAdapter";
		private string	parameterClass		= "FirebirdSql.Data.Firebird.FbParameter";
		
		#endregion

		#region PROPERTIES

		public int UserNumber
		{
			get { return userNumber; }
			set { userNumber = value; }
		}

		public long DataSize
		{
			get { return dataSize; }
			set { dataSize = value; }
		}

		public string RunSequence
		{
			get { return runSequence; }
			set { runSequence = value; }
		}

		public bool RunCreate
		{
			get { return runCreate; }
			set { runCreate = value; }
		}

		public string DataPath
		{
			get { return dataPath; }
			set { dataPath = value; }
		}

		public bool UseIndexes
		{
			get { return useIndexes; }
			set { useIndexes = value; }
		}

		public string ConnectionString
		{
			get { return connectionString; }
			set { connectionString = value; }
		}

		public string ProviderAssembly
		{
			get { return providerAssembly; }
			set { providerAssembly = value; }
		}
		
		public bool SupportsClusteredIndexes
		{
			get { return supportsClusteredIndexes; }
			set { supportsClusteredIndexes = value; } 
		}

		public bool SupportsHashIndexes
		{
			get { return supportsHashIndexes; }
			set { supportsHashIndexes = value; } 
		}

		public string ConnectionClass
		{
			get { return connectionClass; }
			set { connectionClass = value; }
		}

		public string CommandClass
		{
			get { return commandClass; }
			set { commandClass = value; }
		}

		public string DataAdapterClass
		{
			get { return dataAdapterClass; }
			set { dataAdapterClass = value; }
		}

		public string ParameterClass
		{
			get { return parameterClass; }
			set { parameterClass = value; }
		}

		#endregion

		#region SOAP_METHODS

		public static BenchMarkConfiguration Load(string fileName)
		{
			try
			{
				BenchMarkConfiguration configuration;
			
				// test if file exists.
				if(File.Exists(fileName))
				{
					// read back serialized objectinstance
					FileStream		output		= new FileStream(fileName, FileMode.Open);
					SoapFormatter	formatter	= new SoapFormatter();

					configuration = (BenchMarkConfiguration)formatter.Deserialize(output);
					output.Close();
				}
				else
				{
					// return new instance
					configuration = new BenchMarkConfiguration();
				}

				return configuration;
			}
			catch
			{
				// General error, bubble
				throw;
			}
		}

		public void Save(string fileName)
		{
			// serialize the current instance
			FileStream		output		= new FileStream(fileName, FileMode.Create);
			SoapFormatter	formatter	= new SoapFormatter();

			formatter.Serialize(output, this);
			output.Close();
		}

		#endregion
	}
}
