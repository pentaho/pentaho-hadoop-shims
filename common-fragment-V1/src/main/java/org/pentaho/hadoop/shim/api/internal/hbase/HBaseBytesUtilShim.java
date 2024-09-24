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

package org.pentaho.hadoop.shim.api.internal.hbase;

public interface HBaseBytesUtilShim {

  int getSizeOfFloat();

  int getSizeOfDouble();

  int getSizeOfInt();

  int getSizeOfLong();

  int getSizeOfShort();

  int getSizeOfByte();

  byte[] toBytes( String aString );

  byte[] toBytes( int anInt );

  byte[] toBytes( long aLong );

  byte[] toBytes( float aFloat );

  byte[] toBytes( double aDouble );

  byte[] toBytesBinary( String value );

  String toString( byte[] value );

  long toLong( byte[] value );

  int toInt( byte[] value );

  float toFloat( byte[] value );

  double toDouble( byte[] value );

  short toShort( byte[] value );
}
