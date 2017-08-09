package org.pentaho.hadoop.shim.api.format;


import org.pentaho.hadoop.shim.api.Configuration;

public interface PentahoOutputFormat {

  PentahoRecordWriter getRecordWriter( );

  Configuration getActiveConfiguration( );
}
