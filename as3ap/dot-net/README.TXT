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

	- The last version of he Firebird .Net Data Provider ( at this moment CVS version is needed ).

	- The sources have a build file for NAnt ( http://nant.sourceforge.net/ ), AS3AP.BenchMark.build

	Before launch the build you need to change two things in the nant build file:

	- Change :

	    <property name="FirebirdSql.Data.Firebird.dir" value=""/>    

	in the nant build file to the directory that contains the FirebirdSql.Data.Firebird.dll file.

	- Exec nant in the command line in the directory that contains the build file.

	- You will need to change the DataPath key of the Configuration file to the directory that have the as3ap 
	data files.



Build with Mono:: platform:

	- You nedd Mono:: ( www.go-mono.com ), version > 0.16 ( at this moment CVS Sources ).

	- The sources have a build file called makefile, for build the sources ( this script file needs Cygwin ).

	Note : You can build it using NAnt too, for this you need to modify the NAnt script changing the build 
	file for allow it.


Requirements for build the sources on Linux
---------------------------------------------

Build with Mono:: platform:

	- You need Mono:: ( www.go-mono.com ), version > 0.16 ( at this moment CVS Sources ).

	- The sources have a build file called makefile, for build the sources, you need to modify the directory of 
	the Firebird .Net Provider dll and run make in the directory that contains the makefile.