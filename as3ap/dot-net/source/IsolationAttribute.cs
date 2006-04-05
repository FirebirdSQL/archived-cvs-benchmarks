//
// AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark
//			for Relational Database Systems.
// Copyright (C) 2003-2006  Carlos Guzman Alvarez
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
using System.Data;

namespace DatabaseBenchmark
{
	[AttributeUsage(AttributeTargets.Method)]
	public class IsolationLevelAttribute : System.Attribute 
	{      	
		private IsolationLevel isolationLevel;

		public IsolationLevel IsolationLevel
		{
			get { return isolationLevel; }
		}

		public IsolationLevelAttribute(IsolationLevel isolationLevel)
		{
			this.isolationLevel = isolationLevel;
		}
	}
}
