/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;
import org.pentaho.hadoop.mapreduce.converter.spi.ITypeConverter;

/**
 * Dummy "converter" that passes through Result objects as Object. This will allow them to be carried in PDI rows as
 * TYPE_SERIALIZABLE
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class ResultPassThroughConverter implements ITypeConverter<Result, Object> {

  public boolean canConvert(Class from, Class to) {
    return Result.class.equals(from) && Object.class.equals(to);
  }

  public Object convert( ValueMetaInterface meta, Result obj ) throws TypeConversionException {
    
    // we have to make a copy here because the TableRecordReader's next()
    // method in the mapred package re-uses the Result object. This is fine
    // for sequential apps, but not good for multi-threaded mappers.

    Result newResult = new Result();
    try {

      // Some versions of HBase have removed the Writable interface from Result
      // and added a copyFrom() method. Check for this here, and call the appropriate
      // class and method
      if ( obj instanceof Writable && newResult instanceof Writable ) {
        Writables.copyWritable( (Writable) obj, (Writable) newResult );
      } else {

        Method m = newResult.getClass().getMethod( "copyFrom", Result.class );
        m.invoke( newResult, obj );
      }
    } catch (IOException ex) {
      throw new TypeConversionException( "Problem copying result object!", ex );
    } catch ( NoSuchMethodException ex ) {
      throw new TypeConversionException( "Problem copying result object!", ex );
    } catch ( SecurityException ex ) {
      throw new TypeConversionException( "Problem copying result object!", ex );
    } catch ( IllegalAccessException ex ) {
      throw new TypeConversionException( "Problem copying result object!", ex );
    } catch ( IllegalArgumentException ex ) {
      throw new TypeConversionException( "Problem copying result object!", ex );
    } catch ( InvocationTargetException ex ) {
      throw new TypeConversionException("Problem copying result object!", ex);
    }
    return newResult;
  }

}
