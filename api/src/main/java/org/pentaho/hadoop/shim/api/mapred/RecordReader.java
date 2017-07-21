package org.pentaho.hadoop.shim.api.mapred;

//TODO interface vs class?
//TODO initialize?
//TODO fix K,V ?
public interface RecordReader<K, V> {

  boolean nextKeyValue();

  K getCurrentKey();

  V getCurrentValue();

}
