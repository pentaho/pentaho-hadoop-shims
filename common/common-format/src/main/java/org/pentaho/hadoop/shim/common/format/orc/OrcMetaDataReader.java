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
import org.apache.orc.Reader;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Created by tkafalas on 11/21/2017.
 */
public class OrcMetaDataReader implements IOrcMetaData.Reader {
  private static final Logger logger = Logger.getLogger( OrcMetaDataReader.class );
  Reader reader;

  public OrcMetaDataReader( Reader reader ) {
    this.reader = reader;
  }

  @Override
  public void read( SchemaDescription schemaDescription ) {
    schemaDescription.forEach( field -> {
      try {
        readMetaData( field );
      } catch ( Exception e ) {
        logger.error( "Field " + field.formatFieldName + ": cannot read Orc Metadata" );
      }
    } );
  }

  private void readMetaData( SchemaDescription.Field field ) {
    field.pentahoValueMetaType = readInt( field, IOrcMetaData.propertyType.TYPE );
    field.allowNull = readBoolean( field, IOrcMetaData.propertyType.NULLABLE );
    field.defaultValue = readValue( field, IOrcMetaData.propertyType.DEFAULT );
  }

  private String readValue( SchemaDescription.Field field, IOrcMetaData.propertyType metaField ) {
    String propertyName = IOrcMetaData.determinePropertyName( field.formatFieldName, metaField.toString() );
    if ( reader.hasMetadataValue( propertyName ) ) {
      ByteBuffer b = reader.getMetadataValue( propertyName );
      return b == null ? null : byteBufferToString( b, Charset.forName( "UTF-8" ) );
    }
    return null;
  }

  private int readInt( SchemaDescription.Field field, IOrcMetaData.propertyType metaField ) {
    String s = readValue( field, metaField );
    if ( s != null ) {
      return Integer.valueOf( readValue( field, metaField ) );
    }
    return 0;
  }

  private boolean readBoolean( SchemaDescription.Field field, IOrcMetaData.propertyType metaField ) {
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
