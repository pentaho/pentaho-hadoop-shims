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

package org.pentaho.hadoop.mapreduce.converter.spi;

import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;

/**
 * Provides conversion between types
 *
 * @param <F> Type this converter can convert from
 * @param <T> Type this converter can convert to
 */
public interface ITypeConverter<F, T> {
  /**
   * Can this converter convert between the types provided?
   *
   * @param from Type to convert from
   * @param to   Type to convert to
   * @return {@code true} if this converter can handle the conversion between {@code from} and {@code to}
   */
  public boolean canConvert( Class from, Class to );

  /**
   * Convert an object with some metadata to the destination type.
   *
   * @param meta Metadata for the object provided. This provides hints and formatting to aid in conversion.
   * @param obj  Object to convert
   * @return Converted object
   * @throws org.pentaho.hadoop.mapreduce.converter.TypeConversionException Error encountered when converting {@code
   *                                                                        obj} to type {@code T}
   */
  public T convert( ValueMetaInterface meta, F obj ) throws TypeConversionException;
}
