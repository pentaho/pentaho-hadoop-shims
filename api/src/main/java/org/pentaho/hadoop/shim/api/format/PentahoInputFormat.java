package org.pentaho.hadoop.shim.api.format;

import org.pentaho.hadoop.shim.api.Configuration;

import java.io.IOException;
import java.util.List;

public interface PentahoInputFormat {

  ///??
  //void init( Configuration jobConfiguration, FileSystem path, String schema );

  //void setSchema( String schema );

  List<PentahoInputSplit> getSplits() throws IOException;

  RecordReader getRecordReader( PentahoInputSplit split ) throws IOException, InterruptedException;

  Configuration getActiveConfiguration( );

}
