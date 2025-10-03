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


package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseValueMeta;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by bryan on 1/21/16.
 */
public class ByteConversionUtilImpl implements ByteConversionUtil {
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public ByteConversionUtilImpl( HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
  }

  @Override public int getSizeOfFloat() {
    return hBaseBytesUtilShim.getSizeOfFloat();
  }

  @Override public int getSizeOfDouble() {
    return hBaseBytesUtilShim.getSizeOfDouble();
  }

  @Override public int getSizeOfInt() {
    return hBaseBytesUtilShim.getSizeOfInt();
  }

  @Override public int getSizeOfLong() {
    return hBaseBytesUtilShim.getSizeOfLong();
  }

  @Override public int getSizeOfShort() {
    return hBaseBytesUtilShim.getSizeOfShort();
  }

  @Override public int getSizeOfByte() {
    return hBaseBytesUtilShim.getSizeOfByte();
  }

  @Override public byte[] toBytes( String var1 ) {
    return hBaseBytesUtilShim.toBytes( var1 );
  }

  @Override public byte[] toBytes( int var1 ) {
    return hBaseBytesUtilShim.toBytes( var1 );
  }

  @Override public byte[] toBytes( long var1 ) {
    return hBaseBytesUtilShim.toBytes( var1 );
  }

  @Override public byte[] toBytes( float var1 ) {
    return hBaseBytesUtilShim.toBytes( var1 );
  }

  @Override public byte[] toBytes( double var1 ) {
    return hBaseBytesUtilShim.toBytes( var1 );
  }

  @Override public byte[] toBytesBinary( String var1 ) {
    return hBaseBytesUtilShim.toBytesBinary( var1 );
  }

  @Override public String toString( byte[] var1 ) {
    return hBaseBytesUtilShim.toString( var1 );
  }

  @Override public long toLong( byte[] var1 ) {
    return hBaseBytesUtilShim.toLong( var1 );
  }

  @Override public int toInt( byte[] var1 ) {
    return hBaseBytesUtilShim.toInt( var1 );
  }

  @Override public float toFloat( byte[] var1 ) {
    return hBaseBytesUtilShim.toFloat( var1 );
  }

  @Override public double toDouble( byte[] var1 ) {
    return hBaseBytesUtilShim.toDouble( var1 );
  }

  @Override public short toShort( byte[] var1 ) {
    return hBaseBytesUtilShim.toShort( var1 );
  }

  @Override public byte[] encodeKeyValue( Object keyValue, Mapping.KeyType keyType ) throws KettleException {
    return HBaseValueMeta
      .encodeKeyValue( keyValue, org.pentaho.hadoop.shim.api.internal.hbase.Mapping.KeyType.valueOf( keyType.name() ),
        hBaseBytesUtilShim );
  }

  @Override public byte[] encodeObject( Object obj ) throws IOException {
    return HBaseValueMeta.encodeObject( obj );
  }

  @Override public byte[] compoundKey( String... keys ) {
    StringBuilder stringBuilder = new StringBuilder();
    for ( String key : keys ) {
      stringBuilder.append( key );
      stringBuilder.append( HBaseValueMeta.SEPARATOR );
    }
    if ( stringBuilder.length() > 0 ) {
      stringBuilder.setLength( stringBuilder.length() - HBaseValueMeta.SEPARATOR.length() );
    }
    return toBytes( stringBuilder.toString() );
  }

  @Override public String[] splitKey( byte[] compoundKey ) {
    return toString( compoundKey ).split( HBaseValueMeta.SEPARATOR );
  }

  @Override public String objectIndexValuesToString( Object[] values ) {
    return HBaseValueMeta.objectIndexValuesToString( values );
  }

  @Override public Object[] stringIndexListToObjects( String list ) {
    return HBaseValueMeta.stringIndexListToObjects( list );
  }

  @Override public byte[] encodeKeyValue( Object o, ValueMetaInterface valueMetaInterface, Mapping.KeyType keyType )
    throws KettleException {
    return HBaseValueMeta
      .encodeKeyValue( o, valueMetaInterface,
        org.pentaho.hadoop.shim.api.internal.hbase.Mapping.KeyType.valueOf( keyType.name() ),
        hBaseBytesUtilShim );
  }

  @Override public boolean isImmutableBytesWritable( Object o ) {
    return o instanceof ImmutableBytesWritable;
  }

  @Override public Object convertToImmutableBytesWritable( Object o )
    throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    return new ImmutableBytesWritable(
      (byte[]) o.getClass().getMethod( "copyBytes" ).invoke( o ),
      (Integer) o.getClass().getMethod( "getOffset" ).invoke( o ),
      (Integer) o.getClass().getMethod( "getLength" ).invoke( o ) );
  }
}
