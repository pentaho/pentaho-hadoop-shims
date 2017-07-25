package org.pentaho.hadoop.shim.api.format;


import org.pentaho.hadoop.shim.api.Configuration;

public interface OutputFormat {
  void setSchema( String schema );

  RecordWriter getRecordWriter( Configuration jobConfiguration );

}
