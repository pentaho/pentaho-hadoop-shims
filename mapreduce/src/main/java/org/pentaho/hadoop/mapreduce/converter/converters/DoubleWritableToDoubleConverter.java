/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.hadoop.mapreduce.converter.converters;

import org.apache.hadoop.io.DoubleWritable;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts {@link DoubleWritable} objects to {@link Double} objects
 */
public class DoubleWritableToDoubleConverter implements ITypeConverter<DoubleWritable, Double> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return DoubleWritable.class.equals( from ) && Double.class.equals( to );
  }

  @Override
  public Double convert( ValueMetaInterface meta, DoubleWritable obj ) throws TypeConversionException {
    return new Double( obj.get() );
  }
}
