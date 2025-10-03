/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.impl.shim.pig;

import org.apache.logging.log4j.core.Appender;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.big.data.impl.shim.logging.WriterAppenderManager;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 10/1/15.
 */
public class WriterAppenderManagerTest {
  private LogChannelInterface logChannelInterface;
  private LogLevel logLevel;
  private String testName;
  private WriterAppenderManager writerAppenderManager;

  @Before
  public void setup() {
    logChannelInterface = mock( LogChannelInterface.class );
    logLevel = LogLevel.DETAILED;
    testName = "testName";
    writerAppenderManager = new WriterAppenderManager( logChannelInterface, logLevel, testName, new String[0] );
  }

  @Test
  public void testConstructorAndClose() throws IOException {
    assertNotNull( writerAppenderManager.getFile() );
    writerAppenderManager.close();
  }

  @Test
  public void testError() throws IOException {
    ArgumentCaptor<Appender> captor = ArgumentCaptor.forClass( Appender.class );
    writerAppenderManager = new WriterAppenderManager( logChannelInterface, logLevel, testName, new String[0] );
    assertNotNull( writerAppenderManager.getFile() );
    writerAppenderManager.close();
  }

  @Test
  public void testFactory() throws IOException {
    WriterAppenderManager writerAppenderManager =
      new WriterAppenderManager.Factory().create( logChannelInterface, logLevel, testName, new String[0] );
    assertNotNull( writerAppenderManager.getFile() );
    writerAppenderManager.close();
  }
}
