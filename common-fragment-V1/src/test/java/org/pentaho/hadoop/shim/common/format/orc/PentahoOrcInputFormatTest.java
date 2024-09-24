/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Created by tkafalas on 11/20/2017.
 */
public class PentahoOrcInputFormatTest {
  private PentahoOrcInputFormat pentahoOrcInputFormat;
  private List<IOrcInputField> mockSchemaDescription;

  @Before
  public void setup() throws Exception {
    KettleLogStore.init();
    pentahoOrcInputFormat = new PentahoOrcInputFormat( mock( NamedCluster.class ) );
    mockSchemaDescription = new ArrayList<>();
  }

  @Test( expected = NullPointerException.class )
  public void testCreateRecordReaderWithNoFile() {
    pentahoOrcInputFormat.setSchema( mockSchemaDescription );
    pentahoOrcInputFormat.createRecordReader( null );
  }

  @Test( expected = NullPointerException.class )
  public void testCreateRecordReaderWithNoSchema() {
    pentahoOrcInputFormat.setInputFile( "testFile" );
    pentahoOrcInputFormat.createRecordReader( null );
  }

  @Test
  public void nullNamedClusterIsAllowed() {
    Assert.assertNotNull( new PentahoOrcInputFormat( null ),
      "null named cluster is allowed for non-hadoop filesystems." );
  }
}
