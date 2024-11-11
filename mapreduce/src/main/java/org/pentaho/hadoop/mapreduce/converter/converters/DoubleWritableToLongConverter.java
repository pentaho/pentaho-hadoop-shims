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

import org.apache.hadoop.io.DoubleWritable;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts {@link DoubleWritable} objects to {@link Long} objects
 */
public class DoubleWritableToLongConverter implements ITypeConverter<DoubleWritable, Long> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return DoubleWritable.class.equals( from ) && Long.class.equals( to );
  }

  @Override
  public Long convert( ValueMetaInterface meta, DoubleWritable obj ) throws TypeConversionException {
    return Long.valueOf( (long) obj.get() );
  }
}
