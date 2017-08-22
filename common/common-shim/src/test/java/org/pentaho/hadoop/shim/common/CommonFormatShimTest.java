/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.hadoop.shim.common;

import java.io.IOException;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.PentahoRecordReader;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat;

/**
 * Created by Vasilina_Terehova on 7/27/2017.
 */
public class CommonFormatShimTest {

  @Test
  public void testParquetReadSuccessLocalFileSystem() throws IOException, InterruptedException {
    try {
      SchemaDescription s = new SchemaDescription();
      s.addField( s.new Field( "Name", "b", ValueMetaInterface.TYPE_STRING, true ) );
      s.addField( s.new Field( "Age", "c", ValueMetaInterface.TYPE_INTEGER, true ) );

      PentahoParquetInputFormat pentahoParquetInputFormat = new PentahoParquetInputFormat();
      pentahoParquetInputFormat.setSchema( s );
      pentahoParquetInputFormat.setInputFile( CommonFormatShimTest.class.getClassLoader().getResource( "sample.pqt" )
          .toExternalForm() );
      PentahoRecordReader recordReader =
          pentahoParquetInputFormat.createRecordReader( pentahoParquetInputFormat.getSplits().get( 0 ) );
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
