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


package org.pentaho.hadoop.mapreduce.converter.converters;

import org.apache.hadoop.io.BytesWritable;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts {@link BytesWritable} objects to {@link byte[]} objects
 */
public class BytesWritableToByteArrayConverter implements ITypeConverter<BytesWritable, byte[]> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return BytesWritable.class.equals( from ) && byte[].class.equals( to );
  }

  @Override
  public byte[] convert( ValueMetaInterface meta, BytesWritable obj ) throws TypeConversionException {
    return obj.getBytes().clone();
  }
}
