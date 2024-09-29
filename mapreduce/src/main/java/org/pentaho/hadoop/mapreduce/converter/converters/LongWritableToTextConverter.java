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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts {@link LongWritable} objects to {@link Text} objects
 */
public class LongWritableToTextConverter implements ITypeConverter<LongWritable, Text> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return LongWritable.class.equals( from ) && Text.class.equals( to );
  }

  @Override
  public Text convert( ValueMetaInterface meta, LongWritable obj ) throws TypeConversionException {
    Text result = new Text();
    result.set( String.valueOf( obj.get() ) );
    return result;
  }
}
