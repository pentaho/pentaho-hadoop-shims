package org.pentaho.hadoop.shim.api.format;

import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.mapred.InputSplit;
import org.pentaho.hadoop.shim.api.mapred.RecordReader;

//TODO fix K,V ?
//TODO change interface
public interface InputFormat<K, V> {

  InputSplit[] getSplits( Configuration jobConfiguration );

  RecordReader<K, V> getRecordReader( InputSplit split, Configuration jobConfiguration );

}
