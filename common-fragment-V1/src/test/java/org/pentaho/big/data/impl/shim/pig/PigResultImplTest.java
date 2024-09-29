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


package org.pentaho.big.data.impl.shim.pig;

import org.apache.commons.vfs2.FileObject;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 7/14/15.
 */
public class PigResultImplTest {
  @Test
  public void testConstructor() {
    FileObject logFile = mock( FileObject.class );
    int[] result = new int[] { 1, 2, 3 };
    Exception exception = mock( Exception.class );
    PigResultImpl pigResult = new PigResultImpl( logFile, result, exception );
    assertEquals( logFile, pigResult.getLogFile() );
    assertArrayEquals( result, pigResult.getResult() );
    assertEquals( exception, pigResult.getException() );
  }
}
