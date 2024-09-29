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

import org.apache.hadoop.io.BytesWritable;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts any Kettle object to an {@link BytesWritable} object
 */
public class KettleTypeToBytesWritableConverter implements ITypeConverter<Object, BytesWritable> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return TypeConverterFactory.isKettleType( from ) && BytesWritable.class.equals( to );
  }

  @Override
  public BytesWritable convert( ValueMetaInterface meta, Object obj ) throws TypeConversionException {
    try {
      BytesWritable result = new BytesWritable();
      byte[] binary = meta.getBinary( obj );
      result.set( binary, 0, binary.length );
      return result;
    } catch ( Exception ex ) {
      throw new TypeConversionException( BaseMessages
        .getString( TypeConverterFactory.class, "ErrorConverting", BytesWritable.class.getSimpleName(), obj ), ex );
    }
  }
}
