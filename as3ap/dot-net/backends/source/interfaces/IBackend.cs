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
		bool AutoCommit
		{
			get;
			set;
		}

		IsolationLevel Isolation
		{
			get;
			set;
		}

		IDataReader Cursor
		{
			get;
		}

		int  CountTuples(string table);
		void CreateIndexBtree(string iName, string tName, string fields);
		void CreateIndexCluster(string iName, string tName, string fields);
		void CreateIndexForeign(string tName, string keyName, string keyCol,
								string fTable, string fFields);
		void CreateIndexHash(string iName, string tName, string fields);
		void CreateTable(string stg);
		void CursorOpen(string stg);
		bool CursorFetch();
		void CursorClose();		
		void DatabaseConnect();
		void DatabaseCreate(string dName);
		void DatabaseDisconnect();
		void ddl(string stg);
		void dml(string stg);
		int  Load();
		void TransactionBegin();
		void TransactionCommit();
		void TransactionRollback();
	}
}