# DistributedFileSystem

This version has only ping Implementation tested locally
TimeStamp: Nov 20/ 4:30PM


Riak DB:
View list of keys:
http://localhost:8098/buckets/FluffyRiakDatabase/keys?keys=true 

View the value for a key:
http://localhost:8098/buckets/FluffyRiakDatabase/keys/samples.pdf 

To delete a key:
curl -v -X DELETE http://127.0.0.1:8098/buckets/FluffyRiakDatabase/keys/samples.pdf //


Protobuf compilation
./build_pb.sh /usr/local/Cellar/protobuf260/2.6.0
