AS3AP.BenchMark.exe 		= AS3AP.BenchMark.exe
AS3AP.BenchMark.Backends.dll  = AS3AP.BenchMark.Backends.dll
AS3AP.Logger.dll    		= Logger.dll

all: cp-dll ${AS3AP.Logger.dll} ${AS3AP.BenchMark.Backends.dll} ${AS3AP.BenchMark.exe} install clean

MCS = mcs
COPY = cp

LD_FLAGS = -reference:System.dll -reference:System.Data.dll -reference:System.Xml.dll

cp-dll:
	$(COPY) ../asemblys/*.dll ./
	$(COPY) ../../Net-Provider/build/FirebirdSql.Data.Firebird.dll ./

Logger.dll:	
	$(MCS) -target:library -out:Logger.dll ${LD_FLAGS} -recurse:./logger/*.cs


AS3AP.BenchMark.Backends.dll:
	$(MCS) -target:library -out:AS3AP.BenchMark.Backends.dll -recurse:./backends/*.cs ${LD_FLAGS} -reference:FirebirdSql.Data.Firebird.dll -reference:Logger.dll


AS3AP.BenchMark.exe:
	$(MCS) -target:exe -out:AS3AP.BenchMark.exe -recurse:./source/*.cs $(LD_FLAGS) -reference:AS3AP.BenchMark.Backends.dll -reference:Logger.dll

install:
	rm -rf build
	mkdir -p build
	$(COPY) *.exe ./build
	$(COPY) *.dll ./build
	$(COPY) ./source/AS3AP.BenchMark.exe.config ./build
	$(COPY) ./data/as3ap.fdb ./build

clean: 
	rm -rf AS3AP.BenchMark.exe
	rm -rf *.dll