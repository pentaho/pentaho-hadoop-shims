/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.orc;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import static org.mockito.Mockito.mock;

/**
 * Created by tkafalas on 11/20/2017.
 */
public class PentahoOrcInputFormatTest {
  PentahoOrcInputFormat pentahoOrcInputFormat;
  SchemaDescription mockSchemaDescription;
  String fileName = "testFile";

  @Before
  public void setup() throws Exception {
    pentahoOrcInputFormat = new PentahoOrcInputFormat();
    mockSchemaDescription = mock( SchemaDescription.class );
  }

  @Test( expected = IllegalStateException.class )
  public void testCreateRecordReaderWithNoFile() throws Exception {
    pentahoOrcInputFormat.setSchema( mockSchemaDescription );
    pentahoOrcInputFormat.createRecordReader( null );
  }

  @Test( expected = IllegalStateException.class )
  public void testCreateRecordReaderWithNoSchema() throws Exception {
    pentahoOrcInputFormat.setInputFile( fileName );
    pentahoOrcInputFormat.createRecordReader( null );
  }
}
