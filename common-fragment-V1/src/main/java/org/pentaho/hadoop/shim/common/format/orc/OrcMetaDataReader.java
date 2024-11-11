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
import org.apache.orc.Reader;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Created by tkafalas on 11/21/2017.
 */
public class OrcMetaDataReader implements IOrcMetaData.Reader {
  private static final Logger logger = LogManager.getLogger( OrcMetaDataReader.class );
  Reader reader;

  public OrcMetaDataReader( Reader reader ) {
    this.reader = reader;
  }

  @Override
  public void read( List<? extends IOrcInputField> inputFields ) {
    inputFields.forEach( field -> {
      try {
        readMetaData( field );
      } catch ( Exception e ) {
        logger.error( "Field " + field.getFormatFieldName() + ": cannot read Orc Metadata" );
      }
    } );
  }

  private void readMetaData( IOrcInputField inputField ) {
    inputField.setPentahoType( readInt( inputField, IOrcMetaData.propertyType.TYPE ) );
  }

  private String readValue( IOrcInputField inputField, IOrcMetaData.propertyType metaField ) {
    String propertyName = IOrcMetaData.determinePropertyName( inputField.getFormatFieldName(), metaField.toString() );
    if ( reader.hasMetadataValue( propertyName ) ) {
      ByteBuffer b = reader.getMetadataValue( propertyName );
      return b == null ? null : byteBufferToString( b, Charset.forName( "UTF-8" ) );
    } else {
      return String.valueOf( inputField.getPentahoType() );
    }
  }

  private int readInt( IOrcInputField inputField, IOrcMetaData.propertyType metaField ) {
    String s = readValue( inputField, metaField );
    if ( s != null ) {
      return Integer.valueOf( readValue( inputField, metaField ) );
    }
    return 0;
  }

  private boolean readBoolean( IOrcInputField field, IOrcMetaData.propertyType metaField ) {
    String s = readValue( field, metaField );
    if ( s != null ) {
      return Boolean.valueOf( readValue( field, metaField ) );
    }
    return false;
  }

  private String byteBufferToString( ByteBuffer buffer, Charset charset ) {
    byte[] bytes;
    if ( buffer.hasArray() ) {
      bytes = buffer.array();
    } else {
      bytes = new byte[ buffer.remaining() ];
      buffer.get( bytes );
    }
    return new String( bytes, charset );
  }

}
