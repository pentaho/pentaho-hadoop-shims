package org.pentaho.hadoop.shim.common;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.format.InputFormat;
import org.pentaho.hadoop.shim.api.format.PentahoOutputFormat;
import org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat;
import org.pentaho.hadoop.shim.common.fs.FileSystemProxy;
import org.pentaho.hadoop.shim.spi.FormatShim;

public class CommonFormatShim implements FormatShim {


  @Override public InputFormat createInputFormat( FormatType type, Configuration configuration ) {
    //here some factory that will create
    /*if ( type == FormatType.AVRO ) {
      return new AvroInputFormat();
    } else if ( type == FormatType.ORC ) {
      return new OrcInputFormat();
    } else* if ( type == FormatType.PARQUET ) {
      /*return new PentahoParquetInputFormat();
    }*/
    throw new IllegalArgumentException( "Not supported scheme format" );
  }

  @Override public PentahoOutputFormat createOutputFormat(FormatType type, Configuration configuration ) {
    //configuration.set(FileInputFormat.INPUT_DIR, );
//    return new PentahoParquetInputFormat(configuration, schemaDescription,
//            new FileSystemProxy(FileSystem.get(configuration)));
    return null;
  }

  @Override public ShimVersion getVersion() {
    return null;
  }
}
