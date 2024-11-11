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

import org.apache.hadoop.io.BooleanWritable;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts any Kettle object to an {@link BooleanWritable} object
 */
public class KettleTypeToBooleanWritableConverter implements ITypeConverter<Object, BooleanWritable> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return TypeConverterFactory.isKettleType( from ) && BooleanWritable.class.equals( to );
  }

  @Override
  public BooleanWritable convert( ValueMetaInterface meta, Object obj ) throws TypeConversionException {
    try {
      BooleanWritable result = new BooleanWritable();
      result.set( meta.getBoolean( obj ) );
      return result;
    } catch ( Exception ex ) {
      throw new TypeConversionException( BaseMessages
        .getString( TypeConverterFactory.class, "ErrorConverting", BooleanWritable.class.getSimpleName(), obj ), ex );
    }
  }
}
