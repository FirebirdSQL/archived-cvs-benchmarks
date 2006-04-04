//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
// Copyright (C) 2003-2004  Carlos Guzman Alvarez
//
// Distributable under LGPL license.
// You may obtain a copy of the License at http://www.gnu.org/copyleft/lesser.html
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
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
		#region · Fields ·

		private int		userNumber			= 0;
		private string	runSequence			= "SQL87;singleuser;SQL92;singleuser";
		private bool	runCreate			= true;
		private string	dataPath			= String.Empty;
		private bool	useIndexes			= true;
		private bool	enableLogging		= true;
		private bool	enableErrorLogging	= true;

		private string	connectionStringName= "";

		private string	providerName	    = "";
		private bool	supportsClusteredIndexes = false;
		private bool	supportsHashIndexes = false;
		
		private string	btreeIndexStmt		= "create index @INDEX_NAME on @TABLE_NAME (@INDEX_FIELDS)";
		private string	clusteredIndexStmt	= "create unique clustered index @INDEX_NAME on @TABLE_NAME (@INDEX_FIELDS)";
		private string	hashIndexStmt		= "create index @INDEX_NAME on @TABLE_NAME (@INDEX_FIELDS)";
	
		private string	charTypeName		= "char";
		private string	varcharTypeName		= "varchar";
		private string	integerTypeName		= "int";
		private string	decimalTypeName		= "decimal";
		private string	floatTypeName		= "float";
		private string	doubleTypeName		= "double precision";

		private bool	forcedWrites		= true;

		#endregion

		#region · Properties ·

		public int UserNumber
		{
            get { return this.userNumber; }
            set { this.userNumber = value; }
		}

		public string RunSequence
		{
            get { return this.runSequence; }
            set { this.runSequence = value; }
		}

		public bool RunCreate
		{
            get { return this.runCreate; }
            set { this.runCreate = value; }
		}

		public string DataPath
		{
            get { return this.dataPath; }
            set { this.dataPath = value; }
		}

		public bool UseIndexes
		{
            get { return this.useIndexes; }
            set { this.useIndexes = value; }
		}

		public bool	EnableLogging
		{
            get { return this.enableLogging; }
            set { this.enableLogging = value; }
		}

		public bool	EnableErrorLogging
		{
            get { return this.enableErrorLogging; }
            set { this.enableErrorLogging = value; }
		}

		public string ConnectionStringName
		{
			get { return this.connectionStringName; }
            set { this.connectionStringName = value; }
		}

		public string ProviderName
		{
			get { return this.providerName; }
			set { this.providerName = value; }
		}
		
		public bool SupportsClusteredIndexes
		{
            get { return this.supportsClusteredIndexes; }
            set { this.supportsClusteredIndexes = value; } 
		}

		public bool SupportsHashIndexes
		{
            get { return this.supportsHashIndexes; }
            set { this.supportsHashIndexes = value; } 
		}

		public string BtreeIndexStmt
		{
            get { return this.btreeIndexStmt; }
            set { this.btreeIndexStmt = value; }
		}
		
		public string ClusteredIndexStmt
		{
            get { return this.clusteredIndexStmt; }
            set { this.clusteredIndexStmt = value; }
		}

		public string HashIndexStmt
		{
            get { return this.hashIndexStmt; }
            set { this.hashIndexStmt = value; }
		}

		public string CharTypeName
		{
            get { return this.charTypeName; }
            set { this.charTypeName = value; }
		}

		public string VarcharTypeName
		{
            get { return this.varcharTypeName; }
            set { this.varcharTypeName = value; }
		}

		public string IntegerTypeName
		{
            get { return this.integerTypeName; }
            set { this.integerTypeName = value; }
		}

		public string DecimalTypeName
		{
            get { return this.decimalTypeName; }
            set { this.decimalTypeName = value; }
		}

		public string FloatTypeName
		{
            get { return this.floatTypeName; }
            set { this.floatTypeName = value; }
		}

		public string DoubleTypeName
		{
            get { return this.doubleTypeName; }
            set { this.doubleTypeName = value; }
		}

		public bool ForcedWrites
		{
            get { return this.forcedWrites; }
            set { this.forcedWrites = value; }
		}

		#endregion

		#region · SOAP Methods ·

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
