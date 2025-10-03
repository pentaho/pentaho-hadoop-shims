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

package org.pentaho.hadoop.shim.common.format.orc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.Writer;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;
import org.pentaho.hadoop.shim.api.format.IOrcOutputField;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by tkafalas on 11/21/2017.
 */
public class OrcMetaDataWriter implements IOrcMetaData.Writer {
  private static final Logger logger = LogManager.getLogger( OrcMetaDataWriter.class );
  private Writer writer;


  public OrcMetaDataWriter( Writer writer ) {
    this.writer = writer;
  }

  @Override
  public void write( List<? extends IOrcOutputField> fields ) {
    fields.forEach( field -> {
      try {
        setMetaData( field );
      } catch ( UnsupportedEncodingException e ) {
        logger.error( "Field " + field.getPentahoFieldName() + ": cannot set Orc Metadata" );
      }
    } );
  }

  private void setMetaData( IOrcOutputField field ) throws UnsupportedEncodingException {
    addMetaData( field, IOrcMetaData.propertyType.TYPE, toByteBuffer( field.getPentahoFieldName() ) );
    addMetaData( field, IOrcMetaData.propertyType.NULLABLE, toByteBuffer( field.getAllowNull() ) );
    if ( field.getDefaultValue() != null ) {
      addMetaData( field, IOrcMetaData.propertyType.DEFAULT, toByteBuffer( field.getDefaultValue() ) );
    }
  }

  private void addMetaData( IOrcOutputField field, IOrcMetaData.propertyType propertyType, ByteBuffer valueBuffer ) {
    writer.addUserMetadata( IOrcMetaData.determinePropertyName( field.getPentahoFieldName(), propertyType.toString() ),
      valueBuffer );
  }

  private ByteBuffer toByteBuffer( int i ) throws UnsupportedEncodingException {
    return toByteBuffer( String.valueOf( i ) );
  }

  private ByteBuffer toByteBuffer( String s ) throws UnsupportedEncodingException {
    return ByteBuffer.wrap( s.getBytes( "UTF-8" ) );
  }

  private ByteBuffer toByteBuffer( boolean b ) throws UnsupportedEncodingException {
    return ByteBuffer.wrap( String.valueOf( b ).getBytes( "UTF-8" ) );
  }
}
