package org.pentaho.hadoop.shim.common;


import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.format.InputFormat;
import org.pentaho.hadoop.shim.api.format.OutputFormat;
import org.pentaho.hadoop.shim.common.format.AvroInputFormat;
import org.pentaho.hadoop.shim.common.format.OrcInputFormat;
import org.pentaho.hadoop.shim.common.format.ParquetInputFormat;
import org.pentaho.hadoop.shim.spi.FormatShim;

public class CommonFormatShim implements FormatShim {


  @Override public InputFormat createInputFormat( FormatType type ) {
    //here some factory that will create
    if ( type == FormatType.AVRO ) {
      return new AvroInputFormat();
    } else if ( type == FormatType.ORC ) {
      return new OrcInputFormat();
    } else if ( type == FormatType.PARQUET ) {
      return new ParquetInputFormat();
    }
    throw new IllegalArgumentException( "Not supported scheme format" );
  }

  @Override public OutputFormat createOutputFormat( FormatType type ) {
    return null;
  }

  @Override public ShimVersion getVersion() {
    return null;
  }
}
