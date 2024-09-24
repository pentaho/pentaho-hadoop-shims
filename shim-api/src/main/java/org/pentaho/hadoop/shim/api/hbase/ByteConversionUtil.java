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

package org.pentaho.hadoop.shim.api.hbase;

import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by bryan on 1/19/16.
 */
public interface ByteConversionUtil {
  int getSizeOfFloat();

  int getSizeOfDouble();

  int getSizeOfInt();

  int getSizeOfLong();

  int getSizeOfShort();

  int getSizeOfByte();

  byte[] toBytes( String var1 );

  byte[] toBytes( int var1 );

  byte[] toBytes( long var1 );

  byte[] toBytes( float var1 );

  byte[] toBytes( double var1 );

  byte[] toBytesBinary( String var1 );

  String toString( byte[] var1 );

  long toLong( byte[] var1 );

  int toInt( byte[] var1 );

  float toFloat( byte[] var1 );

  double toDouble( byte[] var1 );

  short toShort( byte[] var1 );

  byte[] encodeKeyValue( Object keyValue, Mapping.KeyType keyType ) throws KettleException;

  byte[] encodeObject( Object obj ) throws IOException;

  byte[] compoundKey( String... keys ) throws IOException;

  String[] splitKey( byte[] compoundKey ) throws IOException;

  String objectIndexValuesToString( Object[] values );

  Object[] stringIndexListToObjects( String list );

  byte[] encodeKeyValue( Object o, ValueMetaInterface valueMetaInterface, Mapping.KeyType keyType )
    throws KettleException;

  boolean isImmutableBytesWritable( Object o );

  Object convertToImmutableBytesWritable( Object o )
    throws InvocationTargetException, IllegalAccessException, NoSuchMethodException;
}
