DIVYANK PRATAP TIWARI - 213050029
PARAMVEER CHOUDHARY  -213050061


steps to build and run :

protobuf and cmake should be installed in system
run following commands in linux:

1. export MY_INSTALL_DIR=$HOME/.local
2. mkdir -p $MY_INSTALL_DIR
3.export PATH="$MY_INSTALL_DIR/bin:$PATH"
4.sudo apt install -y build-essential autoconf libtool pkg-config

the following command  install grpc and protocol buffers locally

 cd grpc
$ mkdir -p cmake/build
$ pushd cmake/build
$ cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
      ../..
$ make -j
$ make install
$ popd



From the folder where Cmakelists.txt file is present run following 4 commands

$ mkdir -p cmake/build
$ pushd cmake/build
$ cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ../..
$ make -j


run server and client:

./greeter_async_server
./greeter_async_client



for batch mode of client put all commands in a file named cmd.txt
configfile is named config.txt
