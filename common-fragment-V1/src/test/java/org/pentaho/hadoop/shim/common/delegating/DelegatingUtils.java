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
