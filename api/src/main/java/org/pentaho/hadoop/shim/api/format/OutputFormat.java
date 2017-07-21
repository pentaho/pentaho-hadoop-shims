package org.pentaho.hadoop.shim.api.format;


import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.mapred.RecordWriter;

public interface OutputFormat<K, V> {

  RecordWriter<K, V> getRecordWriter( FileSystem fileSystem );

}
