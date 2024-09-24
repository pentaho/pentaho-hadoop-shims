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

import org.apache.hadoop.io.Text;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.TypeConverterFactory;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts {@link Text} objects to {@link Integer} objects
 */
public class TextToIntegerConverter implements ITypeConverter<Text, Integer> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return Text.class.equals( from ) && Integer.class.equals( to );
  }

  @Override
  public Integer convert( ValueMetaInterface meta, Text obj ) throws TypeConversionException {
    try {
      return Integer.parseInt( obj.toString() );
    } catch ( NumberFormatException ex ) {
      throw new TypeConversionException(
        BaseMessages.getString( TypeConverterFactory.class, "ErrorConverting", Integer.class.getSimpleName(), obj ),
        ex );
    }
  }
}
