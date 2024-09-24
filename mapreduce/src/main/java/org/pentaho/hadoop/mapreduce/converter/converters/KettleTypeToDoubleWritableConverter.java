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

import org.apache.hadoop.io.DoubleWritable;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts any Kettle object to an {@link DoubleWritable} object
 */
public class KettleTypeToDoubleWritableConverter implements ITypeConverter<Object, DoubleWritable> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return TypeConverterFactory.isKettleType( from ) && DoubleWritable.class.equals( to );
  }

  @Override
  public DoubleWritable convert( ValueMetaInterface meta, Object obj ) throws TypeConversionException {
    try {
      DoubleWritable result = new DoubleWritable();
      result.set( meta.getNumber( obj ) );
      return result;
    } catch ( KettleValueException ex ) {
      throw new TypeConversionException( BaseMessages
        .getString( TypeConverterFactory.class, "ErrorConverting", DoubleWritable.class.getSimpleName(), obj ), ex );
    }
  }
}
