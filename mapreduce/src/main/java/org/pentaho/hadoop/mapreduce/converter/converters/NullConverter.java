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
 * Returns {@code null} for any conversion if either type is {@code null}.
 */
public class NullConverter implements ITypeConverter<Object, Object> {
  @Override
  public boolean canConvert( Class from, Class to ) {
    return from == null || to == null;
  }

  @Override
  public Object convert( ValueMetaInterface meta, Object obj ) throws TypeConversionException {
    return null;
  }
}
