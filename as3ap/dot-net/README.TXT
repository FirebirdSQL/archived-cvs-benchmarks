AS3AP - An ANSI SQL Standard Scalable and Portable Benchmark for Relational Database Systems.
=============================================================================================

Requirements for build the sources on Windows
---------------------------------------------

- The Microsoft .Net Framework or Mono:: platform.

- The Firebird .NET Provider binaries should be located in the assemblys directory.

- The Firebird Helper binaries should be located in the assemblys directory.

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