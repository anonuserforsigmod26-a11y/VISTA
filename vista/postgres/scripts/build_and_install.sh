#!/bin/bash

# Change to this-file-exist-path.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"/

# Default
BASE_DIR=$SCRIPT_DIR"../.."
LIB_DIR=$BASE_DIR"pgsql/lib"
LIBVISTA_DIR="${BASE_DIR}/libvista"
UDF_DIR=$SCRIPT_DIR"../udf"
LIB_HEADER_DIR=""
CONFIGURE_WITH_LIB=NO
GDB=NO
INSTALL_PREREQUISITE=NO
COMPILE_OPTION=""
ENABLE_VISTA_BUFFER=NO
CONFIGURE_DEBUG_FLAGS=""
LIBS_FLAGS=""
LDFLAGS_PATH=""
# Parse parameters.
for i in "$@"
do
case $i in
	-b=*|--base-dir=*)
	BASE_DIR="${i#*=}"
	shift
	;;

	-c=*|--compile-option=*)
	COMPILE_OPTION="${i#*=}"
	shift
	;;

	--configure-with-lib)
	CONFIGURE_WITH_LIB=YES
	shift
	;;

	--install-prerequisites)
	INSTALL_PREREQUISITE=YES
	shift
	;;

	--debug)
	DEBUG=YES
	shift
	;;

    --enable-vista-buffer)
    ENABLE_VISTA_BUFFER=YES
    shift
    ;;

	*)
		# unknown option
	;;
esac
done

# Install prerequisites
if [ "$INSTALL_PREREQUISITE" == "YES" ]
then
	sudo apt-get install -y libreadline-dev llvm-14 clang-14
fi

# Set compiler to clang
export CXX=gcc
export CLANG=clang-14

SOURCE_DIR=$BASE_DIR"/postgres"
TARGET_DIR=$BASE_DIR"/pgsql"

cd $SOURCE_DIR

echo "target directory: $TARGET_DIR"

# gdb
if [ "$DEBUG" == "YES" ]
then
COMPILE_OPTION+=" -ggdb -O0 -g3 -fno-omit-frame-pointer"
CONFIGURE_DEBUG_FLAGS="--enable-debug --enable-cassert"
else
COMPILE_OPTION+=" -O2"
fi

if [ "$ENABLE_VISTA_BUFFER" == "YES" ]
then
COMPILE_OPTION+=" -DVISTA -I${LIBVISTA_DIR}/include"
LIBS_FLAGS+="-lvista -lnuma -luring"
LDFLAGS_PATH+="-L${LIBVISTA_DIR} -Wl,-rpath,'${LIBVISTA_DIR}'"
# VISTA_OPTION="--enable-vista-buffer=yes"
fi

# print zicio stats
#COMPILE_OPTION+=" -DZICIO -DZICIO_STAT -I$LIB_HEADER_DIR -lzicio"
echo "compile flags: "$COMPILE_OPTION
# echo "vista option: "$VISTA_OPTION

# configure
if [ "$CONFIGURE_WITH_LIB" == "YES" ]
then
    echo "[VISTA] configure with lib (not used at the moment)"
	./configure --silent --prefix=$TARGET_DIR $CONFIGURE_DEBUG_FLAGS\
		CFLAGS="$COMPILE_OPTION" --with-libs="$LIB_DIR" LIBS="" \
		--with-segsize=10 --with-blocksize=8 \
		--with-llvm LLVM_CONFIG="llvm-config-14" CLANG="clang-14"
else
    echo "[VISTA] basic configure"
     ./configure --silent --prefix=$TARGET_DIR $CONFIGURE_DEBUG_FLAGS CFLAGS="$COMPILE_OPTION" \
	   LDFLAGS="${LDFLAGS_PATH}" LIBS="${LIBS_FLAGS}"
fi

echo "LIBS_FLAGS is: $LIBS_FLAGS"
echo "LDFLAGS is: $LDFLAGS_PATH"


echo "[VISTA] make clean"
make clean -j --silent

echo "[VISTA] make start"
# make
make -j$(nproc) --silent

echo "[VISTA] make install start"
sudo make install -j --silent

echo "[VISTA] make install done"

if [ "$ENABLE_VISTA_BUFFER" == "YES" ]
then
echo "[VISTA] making UDF"
cd $UDF_DIR
make
sudo make install
fi
