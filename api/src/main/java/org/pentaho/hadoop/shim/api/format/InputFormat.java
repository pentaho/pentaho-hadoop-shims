package org.pentaho.hadoop.shim.api.format;

import java.io.IOException;
import java.util.List;

public interface InputFormat {

  ///??
  //void init( Configuration jobConfiguration, FileSystem path, String schema );

  //void setSchema( String schema );

  List<PentahoInputSplit> getSplits() throws IOException;

  RecordReader getRecordReader( PentahoInputSplit split ) throws IOException, InterruptedException;

}
