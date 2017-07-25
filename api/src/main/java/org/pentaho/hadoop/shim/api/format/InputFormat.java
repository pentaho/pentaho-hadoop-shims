package org.pentaho.hadoop.shim.api.format;

import org.pentaho.hadoop.shim.api.Configuration;

public interface InputFormat {
  void setSchema( String schema );

  InputSplit[] getSplits( Configuration jobConfiguration );

  RecordReader getRecordReader( InputSplit split, Configuration jobConfiguration );
}
