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

import org.apache.hadoop.io.NullWritable;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts any type to a {@link NullWritable} object
 */
public class NullWritableConverter implements ITypeConverter<Object, NullWritable> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return NullWritable.class.equals( to );
  }

  @Override
  public NullWritable convert( ValueMetaInterface meta, Object obj ) throws TypeConversionException {
    return NullWritable.get();
  }
}
