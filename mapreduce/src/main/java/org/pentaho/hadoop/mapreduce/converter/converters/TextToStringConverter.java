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
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Converts {@link Text} objects to {@link String} objects
 */
public class TextToStringConverter implements ITypeConverter<Text, String> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return Text.class.equals( from ) && String.class.equals( to );
  }

  @Override
  public String convert( ValueMetaInterface meta, Text obj ) throws TypeConversionException {
    return obj.toString();
  }
}
