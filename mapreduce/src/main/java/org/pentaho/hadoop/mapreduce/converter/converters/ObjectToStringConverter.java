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

import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts any type to a {@link String} object. This is a fail-safe implementation for converting an object to {@link
 * String}.
 */
public class ObjectToStringConverter implements ITypeConverter<Object, String> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return String.class.equals( to );
  }

  @Override
  public String convert( ValueMetaInterface meta, Object obj ) throws TypeConversionException {
    if ( obj == null ) {
      throw new NullPointerException();
    }
    return String.valueOf( obj );
  }
}
