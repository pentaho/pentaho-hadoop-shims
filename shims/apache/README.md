# "Apache" Big Data Format Shim

This shim was developed to handle 
reading and writing of the big data formats (Parquet, ORC and Avro)
to S3, HCP, and the local filesystem.  These are cases where no Hadoop cluster
is in use, but the functionality still relies on Hadoop libraries.
Since there is no named cluster, users should not be required to select a shim. 

S3 and HCP are referenced as named vfs connections within PDI
(e.g. pvfs://mys3conn/bucket/file.txt)
This shim includes a delegating Hadoop FS implementation (PvfsHadoopBridge),
which looks up the named vfs connection from
the metastore.  For HCP and S3 connections, it will update
the Hadoop Configuration based on the saved vfs connection details,
and with both HCP and S3, it will update the URI to be `s3a:\\`.  
HCP supports the org.apache.hadoop.fs.s3a.S3AFileSystem.

HCP VFS in Pentaho includes an option to "Accept self-signed certificates".
In order to support that option with the Hadoop Filesystem,
this shim includes a `SelfSignedS3ClientFactory`.  
The bridge will configure S3AFileSystem to this "trusting" factory if specified. 

This shim does not specify a version in its name, since it should be treated as
an *internal* shim only, and only a single version should be available within PDI.


## Future
Support bridging to Google Cloud Storage

The SelfSignedS3ClientFactory uses com.amazonaws.thirdparty.apache.http.conn.ssl.SSLConnectionSocketFactory,
rather than the apache library directly.  It needs to do this because the aws-java-sdk-bundle
has modified package names for these classes.  If we switch to using the
component libraries, rather than the aws-java-sdk-bundle, we'd need to also
update the SelfSignedS3ClientFactory.

