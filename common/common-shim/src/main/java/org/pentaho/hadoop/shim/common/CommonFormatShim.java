package org.pentaho.hadoop.shim.common;


import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.format.PentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.PentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat;
import org.pentaho.hadoop.shim.common.format.PentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.spi.FormatShim;

public class CommonFormatShim implements FormatShim {


  @Override public PentahoInputFormat createInputFormat( FormatType type, Configuration configuration, SchemaDescription schema ) {
    //here some factory that will create
    /*if ( type == FormatType.AVRO ) {
      return new AvroInputFormat();
    } else if ( type == FormatType.ORC ) {
      return new OrcInputFormat();*/
     if ( type == FormatType.PARQUET ) {
     return new PentahoParquetInputFormat(configuration, schema, null);
    }
    throw new IllegalArgumentException( "Not supported scheme format" );
  }

  @Override public PentahoOutputFormat createOutputFormat(FormatType type, Configuration configuration, SchemaDescription schema ) {
    //configuration.set(FileInputFormat.INPUT_DIR, );
//    return new PentahoParquetInputFormat(configuration, schemaDescription,
//            new FileSystemProxy(FileSystem.get(configuration)));
    if ( type == FormatType.PARQUET ) {
      return new PentahoParquetOutputFormat(configuration, schema);
     }
     throw new IllegalArgumentException( "Not supported scheme format" );
  }

  @Override public ShimVersion getVersion() {
    return null;
  }
}
