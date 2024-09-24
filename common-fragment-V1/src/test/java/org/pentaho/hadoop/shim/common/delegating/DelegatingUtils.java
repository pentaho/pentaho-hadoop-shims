/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.delegating;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * User: Dzmitry Stsiapanau Date: 02/18/2016 Time: 13:43
 */

public class DelegatingUtils {

  public static Object[] createArgs( Class[] types ) throws Exception {
    List<Object> args = new ArrayList<Object>();
    for ( Class type : types ) {
      try {
        if ( type.isPrimitive() ) {
          Object arg = null;
          switch ( type.toString() ) {
            case "boolean": {
              arg = true;
              break;
            }
            case "int": {
              arg = 0;
              break;
            }
            case "short": {
              arg = (short) 1;
              break;
            }
            case "char": {
              arg = ' ';
              break;
            }
            case "long": {
              arg = 0L;
              break;
            }
            case "byte": {
              arg = (byte) 1;
              break;
            }
            case "float": {
              arg = 0F;
              break;
            }
            case "double": {
              arg = 0D;
              break;
            }
          }
          args.add( arg );
        } else if ( Modifier.isFinal( type.getModifiers() ) ) {
          args.add( null );
        } else {
          args.add( mock( type ) );
        }
      } catch ( IllegalArgumentException e ) {
        throw new Exception(
          "Is not able to mock arguments, disable this method and create separate tests foreach delegation "
            + "method", e );
      }
    }
    return args.toArray();
  }
}
