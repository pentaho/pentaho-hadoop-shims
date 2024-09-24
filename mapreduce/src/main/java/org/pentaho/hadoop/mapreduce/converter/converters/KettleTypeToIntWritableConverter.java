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
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts any Kettle object to an {@link IntWritable} object
 */
public class KettleTypeToIntWritableConverter implements ITypeConverter<Object, IntWritable> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return TypeConverterFactory.isKettleType( from ) && IntWritable.class.equals( to );
  }

  @Override
  public IntWritable convert( ValueMetaInterface meta, Object obj ) throws TypeConversionException {
    try {
      IntWritable result = new IntWritable();
      result.set( meta.getInteger( obj ).intValue() );
      return result;
    } catch ( KettleValueException ex ) {
      throw new TypeConversionException(
        BaseMessages.getString( TypeConverterFactory.class, "ErrorConverting", IntWritable.class.getSimpleName(), obj ),
        ex );
    }
  }
}
