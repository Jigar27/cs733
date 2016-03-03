Title: File Server

Build a simple file server, with a simple read/write interface (there’s
no open/delete/rename). The file contents can be in memory, but if you use a database
like leveldb to store the files, you get extra nice points! There are two features that this
file system has that traditional file systems don’t. The first is that each file has a version,
and the API supports a “compare and swap” operation based on the version .
The second is that files can optionally expire after some time.


Assumption:
server runs on port 8080.
In case of shutting server down, data will be lost.

Testing:
server_test.go contains serveral test cases which can be run by issuing "go test" 
