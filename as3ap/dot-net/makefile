AS3AP.BenchMark.exe 		= AS3AP.BenchMark.exe
AS3AP.BenchMark.Backends.dll  	= AS3AP.BenchMark.Backends.dll
AS3AP.Logger.dll    		= AS3AP.Logger.dll

all: cp-dll ${AS3AP.Logger.dll} ${AS3AP.BenchMark.Backends.dll} ${AS3AP.BenchMark.exe} install clean

MCS = mcs
COPY = cp

LD_FLAGS = -reference:System.dll -reference:System.Data.dll -reference:System.Xml.dll

cp-dll:
	$(COPY) /home/linux/desarrollo/Net-Provider/build/FirebirdSql.Data.Firebird.dll ./

AS3AP.Logger.dll:	
	$(MCS) -target:library -out:AS3AP.Logger.dll ${LD_FLAGS} -recurse:./Logger/*.cs


AS3AP.BenchMark.Backends.dll:
	$(MCS) -target:library -out:AS3AP.BenchMark.Backends.dll -recurse:./backends/*.cs ${LD_FLAGS} -reference:FirebirdSql.Data.Firebird.dll -reference:AS3AP.Logger.dll


AS3AP.BenchMark.exe:
	$(MCS) -target:exe -out:AS3AP.BenchMark.exe -recurse:./source/*.cs $(LD_FLAGS) -reference:AS3AP.BenchMark.Backends.dll -reference:AS3AP.Logger.dll

install:
	rm -rf build
	mkdir -p build
	$(COPY) *.exe ./build
	$(COPY) *.dll ./build
	$(COPY) ./source/AS3AP.BenchMark.exe.config ./build
	$(COPY) ./data/AS3AP.FDB ./build

clean: 
	rm -rf AS3AP.BenchMark.exe
	rm -rf *.dll