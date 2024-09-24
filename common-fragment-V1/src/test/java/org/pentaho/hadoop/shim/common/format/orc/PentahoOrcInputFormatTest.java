/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
