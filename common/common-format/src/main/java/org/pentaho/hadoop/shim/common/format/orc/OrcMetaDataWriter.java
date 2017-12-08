/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.log4j.Logger;
import org.apache.orc.Writer;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Created by tkafalas on 11/21/2017.
 */
public class OrcMetaDataWriter implements IOrcMetaData.Writer {
  private static final Logger logger = Logger.getLogger( OrcMetaDataWriter.class );
  private Writer writer;


  public OrcMetaDataWriter( Writer writer ) {
    this.writer = writer;
  }

  @Override
  public void write( SchemaDescription schemaDescription ) {
    schemaDescription.forEach( field -> {
      try {

        setMetaData( field );
      } catch ( UnsupportedEncodingException e ) {
        logger.error( "Field " + field.formatFieldName + ": cannot set Orc Metadata" );
      }
    } );
  }

  private void setMetaData( SchemaDescription.Field field ) throws UnsupportedEncodingException {
    addMetaData( field, IOrcMetaData.propertyType.TYPE, toByteBuffer( field.pentahoValueMetaType ) );
    addMetaData( field, IOrcMetaData.propertyType.NULLABLE, toByteBuffer( field.allowNull ) );
    if ( field.defaultValue != null ) {
      addMetaData( field, IOrcMetaData.propertyType.DEFAULT, toByteBuffer( field.defaultValue ) );
    }
  }

  private void addMetaData( SchemaDescription.Field field, IOrcMetaData.propertyType propertyType, ByteBuffer valueBuffer ) {
    writer.addUserMetadata( IOrcMetaData.determinePropertyName( field.formatFieldName, propertyType.toString() ), valueBuffer );
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
