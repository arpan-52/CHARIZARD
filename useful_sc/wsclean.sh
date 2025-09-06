#!/bin/bash
INSTALL_DIR=/lustre_archive/apps/astrosoft/test_imaging/soft
# mkdir -p $INSTALL_DIR
export PATH=$INSTALL_DIR/bin:$PATH
export LD_LIBRARY_PATH=$INSTALL_DIR/lib:$LD_LIBRARY_PATH
export C_INCLUDE_PATH=$INSTALL_DIR/include
export CPLUS_INCLUDE_PATH=$INSTALL_DIR/include
export LIBRARY_PATH=$INSTALL_DIR/lib

# CMake
CMAKE_VERSION=3.21.4
wget https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION.tar.gz
tar -xzf cmake-$CMAKE_VERSION.tar.gz
cd cmake-$CMAKE_VERSION
./bootstrap --prefix=$INSTALL_DIR
make -j4
make install
cd ..

# FFTW
FFTW_VERSION=3.3.10
wget http://www.fftw.org/fftw-$FFTW_VERSION.tar.gz
tar -xzf fftw-$FFTW_VERSION.tar.gz
cd fftw-$FFTW_VERSION
./configure --prefix=$INSTALL_DIR --enable-float --enable-threads --enable-shared --enable-thread-safe
make -j4
make install
./configure --prefix=$INSTALL_DIR --enable-threads --enable-shared --enable-thread-safe
make -j4
make install
cd ..

# CFITSIO
CFITSIO_VERSION=4.0.0
wget https://heasarc.gsfc.nasa.gov/FTP/software/fitsio/c/cfitsio-$CFITSIO_VERSION.tar.gz
tar -xzf cfitsio-$CFITSIO_VERSION.tar.gz
cd cfitsio-$CFITSIO_VERSION
./configure --prefix=$INSTALL_DIR
make -j4
make install
cd ..

# GSL
GSL_VERSION=2.8
wget https://mirrors.hopbox.net/gnu/gsl/gsl-$GSL_VERSION.tar.gz
tar -xzf gsl-$GSL_VERSION.tar.gz
cd gsl-$GSL_VERSION
./configure --prefix=$INSTALL_DIR
make -j4
make install
cd ..

# HDF5
HDF5_VERSION=1.12.1
wget https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.12/hdf5-$HDF5_VERSION/src/hdf5-$HDF5_VERSION.tar.bz2
tar -xjf hdf5-$HDF5_VERSION.tar.bz2
cd hdf5-$HDF5_VERSION
./configure --prefix=$INSTALL_DIR --enable-cxx --enable-threadsafe
make -j4
make install
cd ..

# Casacore
git clone https://github.com/casacore/casacore.git
cd casacore
mkdir -p build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DUSE_HDF5=ON -DUSE_THREADS=ON
make -j4
make install
cd ../..


# Rebuild Boost with Python
cd boost_1_77_0
./bootstrap.sh --prefix=$INSTALL_DIR --with-python=python3 --with-python-version=3.6
./b2 install --with-python

# Install WSClean
cd ../wsclean-v3.5
mkdir -p build && cd build

# Configure with Python bindings
cmake .. -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
         -DFFTW_ROOT=$INSTALL_DIR \
         -DBoost_INCLUDE_DIR=$INSTALL_DIR/include \
         -DBoost_LIBRARY_DIR=$INSTALL_DIR/lib \
         -DPYTHON_EXECUTABLE=$(which python3)

make -j4
make install

# Update environment
echo "export BOOST_ROOT=$INSTALL_DIR" >> $HOME/.bashrc
source $HOME/.bashrc
# Environment setup
echo "export PATH=$INSTALL_DIR/bin:\$PATH" >> $HOME/.bashrc
echo "export LD_LIBRARY_PATH=$INSTALL_DIR/lib:\$LD_LIBRARY_PATH" >> $HOME/.bashrc
echo "export FFTW_ROOT=$INSTALL_DIR" >> $HOME/.bashrc
source $HOME/.bashrc