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


package org.pentaho.hadoop.mapreduce.converter.converters;

import org.apache.hadoop.io.IntWritable;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts {@link IntWritable} objects to {@link Long} objects
 */
public class IntWritableToLongConverter implements ITypeConverter<IntWritable, Long> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return IntWritable.class.equals( from ) && Long.class.equals( to );
  }

  @Override
  public Long convert( ValueMetaInterface meta, IntWritable obj ) throws TypeConversionException {
    return new Long( obj.get() );
  }
}
