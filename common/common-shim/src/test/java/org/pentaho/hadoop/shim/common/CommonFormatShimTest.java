package org.pentaho.hadoop.shim.common;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.hadoop.shim.api.format.RecordReader;
import org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat;
import org.pentaho.hadoop.shim.common.fs.FileSystemProxy;

import java.io.IOException;

/**
 * Created by Vasilina_Terehova on 7/27/2017.
 */
public class CommonFormatShimTest {

  @Test
  public void testParquetReadSuccessLocalFileSystem() throws IOException, InterruptedException {
    ConfigurationProxy jobConfiguration = new ConfigurationProxy();
    jobConfiguration.set( FileInputFormat.INPUT_DIR, CommonFormatShimTest.class.getClassLoader().getResource("sample.pqt").getFile() );
    PentahoParquetInputFormat pentahoParquetInputFormat = new PentahoParquetInputFormat( jobConfiguration,
      new FileSystemProxy( FileSystem.get( jobConfiguration ) ),
      "message PersonRecord {\n"
        + "required string name;\n"
        + "required string age;\n"
        + "}" );
    RecordReader recordReader = pentahoParquetInputFormat.getRecordReader(pentahoParquetInputFormat.getSplits().get(0));
    recordReader.forEach( rowMetaAndData -> {
      RowMetaInterface rowMeta = rowMetaAndData.getRowMeta();
      for (String fieldName : rowMeta.getFieldNames()) {
        try {
          System.out.println( fieldName + " " + rowMetaAndData.getString( fieldName, "" ) );
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
      }
    } );

  }
}
