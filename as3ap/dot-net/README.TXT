AS3AP -	An ANSI SQL Standard Scalable and Portable Benchmark for Relational Database Systems.
=============================================================================================

License
-------

//
// AS3AP - An ANSI SQL Standard Scalable and Portable Benchmark
// for Relational Database Systems.
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



Requirements for build the sources on Windows
---------------------------------------------

- The Microsoft .Net Framework or Mono:: platform.


Build with Microsoft .Net Framework:

	- You need the Microsoft .Net Platform.

	- The provider sources have a build file for NAnt ( http://nant.sourceforge.net/ ), AS3AP.BenchMark.build,
	before build the sources using Nant you need to change two things on it:

		1.- Change the Html Help Compiler Path, 
		2.- Change the NUnit Path.

	For build it you only need to exececute nant on the same directory as the build file.


Build with Mono:: platform:

	- You nedd Mono:: ( www.go-mono.com ), version > 0.16 ( at this moment CVS Sources ).

	- The provider sources have a build file, makefile, for build the sources ( this script file needs Cygwin ).

	Note : You can build it using NAnt too, for this you need to modify the NAnt script changing the build 
	file for allow it.


Requirements for build the sources on Linux
---------------------------------------------

Build with Mono:: platform:

	- You nedd Mono:: ( www.go-mono.com ), version > 0.16 ( at this moment CVS Sources ).

	- The provider sources have a build file, makefile, for build the sources, you only need to execute make on the same
	directory as the script.