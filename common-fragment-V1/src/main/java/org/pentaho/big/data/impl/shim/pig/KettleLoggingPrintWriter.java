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

import org.pentaho.di.core.logging.LogChannelInterface;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * An extended PrintWriter that sends output to Kettle's logging
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class KettleLoggingPrintWriter extends PrintWriter {
  private final LogChannelInterface logChannelInterface;

  public KettleLoggingPrintWriter( LogChannelInterface logChannelInterface ) {
    this( logChannelInterface, System.out );
  }

  public KettleLoggingPrintWriter( LogChannelInterface logChannelInterface, PrintStream printStream ) {
    super( printStream );
    this.logChannelInterface = logChannelInterface;
  }

  @Override
  public void println( String string ) {
    logChannelInterface.logBasic( string );
  }

  @Override
  public void println( Object obj ) {
    println( obj.toString() );
  }

  @Override
  public void write( String string ) {
    println( string );
  }

  @Override
  public void print( String string ) {
    println( string );
  }

  @Override
  public void print( Object obj ) {
    print( obj.toString() );
  }

  @Override
  public void close() {
    flush();
  }
}
