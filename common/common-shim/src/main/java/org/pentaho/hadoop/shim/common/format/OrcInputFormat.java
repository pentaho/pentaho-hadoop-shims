package org.pentaho.hadoop.shim.common.format;


import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.format.InputFormat;
import org.pentaho.hadoop.shim.api.format.InputSplit;
import org.pentaho.hadoop.shim.api.format.RecordReader;

/**
 * Created by Vasilina_Terehova on 7/25/2017.
 */
public class OrcInputFormat implements InputFormat {

  @Override public void setSchema( String schema ) {

  }

  @Override public InputSplit[] getSplits( Configuration jobConfiguration ) {
    return new InputSplit[ 0 ];
  }

  @Override public RecordReader getRecordReader( InputSplit split, Configuration jobConfiguration ) {
    return null;
  }
}
