package org.pentaho.hadoop.shim.api.mapred;

//TODO class vs interface?
public interface RecordWriter<K, V> {

  void write( K key, V value );

  //TODO autocloseable?
  void close();

}
