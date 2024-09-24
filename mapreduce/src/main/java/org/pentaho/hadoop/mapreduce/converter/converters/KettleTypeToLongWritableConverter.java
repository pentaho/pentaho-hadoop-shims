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
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts any Kettle object to a {@link LongWritable} object
 */
public class KettleTypeToLongWritableConverter implements ITypeConverter<Object, LongWritable> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return TypeConverterFactory.isKettleType( from ) && LongWritable.class.equals( to );
  }

  @Override
  public LongWritable convert( ValueMetaInterface meta, Object obj ) throws TypeConversionException {
    try {
      LongWritable result = new LongWritable();
      result.set( meta.getInteger( obj ) );
      return result;
    } catch ( KettleValueException ex ) {
      throw new TypeConversionException( BaseMessages
        .getString( TypeConverterFactory.class, "ErrorConverting", LongWritable.class.getSimpleName(), obj ), ex );
    }
  }
}
