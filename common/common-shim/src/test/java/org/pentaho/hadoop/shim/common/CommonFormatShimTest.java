package org.pentaho.hadoop.shim.common;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.RecordReader;
import org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat;
import org.pentaho.hadoop.shim.common.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.fs.FileSystemProxy;

/**
 * Created by Vasilina_Terehova on 7/27/2017.
 */
public class CommonFormatShimTest {

  @Test
  public void testParquetReadSuccessLocalFileSystem() throws IOException, InterruptedException {
    try {
      SchemaDescription s = new SchemaDescription();
      s.addField( s.new Field( "Name", "b", ValueMetaInterface.TYPE_STRING ) );
      s.addField( s.new Field( "Age", "c", ValueMetaInterface.TYPE_INTEGER ) );

      ConfigurationProxy jobConfiguration = new ConfigurationProxy();
      jobConfiguration.set( FileInputFormat.INPUT_DIR, CommonFormatShimTest.class.getClassLoader().getResource(
          "sample.pqt" ).getFile() );
      PentahoParquetInputFormat pentahoParquetInputFormat =
          new PentahoParquetInputFormat( jobConfiguration, s, new FileSystemProxy( FileSystem.get(
              jobConfiguration ) ) );
      RecordReader recordReader =
          pentahoParquetInputFormat.getRecordReader( pentahoParquetInputFormat.getSplits().get( 0 ) );
      recordReader.forEach( rowMetaAndData -> {
        RowMetaInterface rowMeta = rowMetaAndData.getRowMeta();
        for ( String fieldName : rowMeta.getFieldNames() ) {
          try {
            System.out.println( fieldName + " " + rowMetaAndData.getString( fieldName, "" ) );
          } catch ( KettleValueException e ) {
            e.printStackTrace();
          }
        }
      } );
    } catch ( Throwable ex ) {
      ex.printStackTrace();
    }
  }
}
