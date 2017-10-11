/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.hadoop.mapreduce.converter.converters;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Dummy "converter" that passes through ImmutableBytesWritable objects as Object. This will allow them to be carried in
 * PDI rows as TYPE_SERIALIZABLE
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class ImmutableBytesWritablePassThroughConverter implements
  ITypeConverter<ImmutableBytesWritable, Object> {

  public boolean canConvert( Class from, Class to ) {
    return ImmutableBytesWritable.class.equals( from ) && Object.class.equals( to );
  }

  public Object convert( ValueMetaInterface meta, ImmutableBytesWritable obj )
    throws TypeConversionException {
    // returning the ImmutableBytesWritable object rather than its payload allows
    // clients to type check
    return obj;
  }
}
