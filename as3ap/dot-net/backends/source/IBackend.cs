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
using System.Data;

namespace AS3AP.BenchMark.Backends
{
	public interface IBackend
	{
		IsolationLevel Isolation
		{
			get;
			set;
		}

		IDataReader Cursor
		{
			get;
		}

		long DataSize
		{
			get;
			set;
		}

		void CreateIndexBtree(string indexName, string tableName, string fields);
		void CreateIndexCluster(string indexName, string tableName, string fields);
		void CreateForeignKey(string foreignTable, string constraintName, 
								string foreignKeyColumns,
								string referencesTableName, 
								string referencesFields);
		void CreateIndexHash(string indexName, string tableName, string fields);
		void CreateTable(string tableName, string tableStructure, string primaryKey);
		void CursorOpen(string statement);
		bool CursorFetch();
		void CursorClose();		
		void DatabaseConnect();
		void DatabaseCreate(string databaseName);
		void DatabaseDisconnect();
		void ExecuteStatement(string statement);
		void CreateData();
		void LoadData();
		void TransactionBegin();
		void TransactionCommit();
		void TransactionRollback();
	}
}