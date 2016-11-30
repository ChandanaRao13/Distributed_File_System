# DistributedFileSystem

##Steps on Installation:##

## Software Installation ##
###a. Install rethinkDB. https://www.rethinkdb.com/docs/install/
###b. Install riak. http://docs.basho.com/riak/kv/2.2.0/setup/installing/
###c. Install Java. https://java.com/en/download/
###d. Install Google Protobuf. https://github.com/google/protobuf
###e. Install Python. https://www.python.org/downloads/

## Server Code Run ##
###f. Clone our project using the following command, git clone https://github.com/MadhuriMotoori/Distributed_File_System.git
###g. Edit your runtime/route-<server-id>.conf -- for local and runtime/global-route-<server-id>.conf -- for global
###h. Run the command in the project's directory to generate the protobuf files. "./build_pb.sh <path-to-protobuf>"
###i. Run the command to launch the servers. "./startServer.sh runtime/route-<server-id>.conf runtime/global-route-<server-id>.conf"

## Client Code Run ##
###j. open python directory in the project folder and run "./build_pb.sh" to generate python protobuf code
###k. open python directory in the project folder and run "./python_dependency_build.sh" to install python dependencies
###l. run the python client by running from the project home directory. "python python/src/FluffyClientApplication.py"
