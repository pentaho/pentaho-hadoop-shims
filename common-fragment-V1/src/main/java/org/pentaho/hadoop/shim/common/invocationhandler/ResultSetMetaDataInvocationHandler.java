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
package org.pentaho.hadoop.shim.common.invocationhandler;

/**
 * User: Dzmitry Stsiapanau Date: 01/17/2017 Time: 15:18
 */

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * ResultSetMetaDataInvocationHandler is a proxy handler class for java.sql.ResultSetMetaData. However the code in this
 * file is specifically for handling Hive JDBC calls, and therefore should not be used to proxy any other JDBC objects
 * besides those provided by Hive.
 */
public class ResultSetMetaDataInvocationHandler implements InvocationHandler {

  /**
   * The "real" ResultSetMetaData object.
   */
  ResultSetMetaData rsmd;

  /**
   * Instantiates a new result set meta data invocation handler.
   *
   * @param r the r
   */
  public ResultSetMetaDataInvocationHandler( ResultSetMetaData r ) {
    rsmd = r;
  }

  /**
   * Intercepts methods called on the ResultSetMetaData object to possibly perform alternate processing.
   *
   * @param proxy  the proxy
   * @param method the method
   * @param args   the args
   * @return the object
   * @throws Throwable the throwable
   */
  @Override
  public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable {
    try {
      String methodName = method.getName();
      if ( ( "getColumnName".equals( methodName ) || ( "getColumnLabel".equals( methodName ) ) ) && ( args != null )
        && ( args.length == 1 ) ) {
        return getColumnName( (Integer) args[ 0 ] );
      }
      return method.invoke( this.rsmd, args );
    } catch ( Throwable t ) {
      if ( ( t instanceof InvocationTargetException ) ) {
        Throwable cause = t.getCause();
        if ( ( cause instanceof SQLException ) ) {
          if ( cause.getMessage().equals( "Method not supported" ) ) {
            String methodName = method.getName();
            if ( "isSigned".equals( methodName ) ) {
              if ( args != null ) {
                return isSigned( (Integer) args[ 0 ] );
              }
            }
            throw cause;
          }
          throw cause;
        }
        throw cause;
      }
      throw t;
    }
  }

  private String getColumnName( Integer column ) throws SQLException {
    String columnName = null;
    columnName = this.rsmd.getColumnName( column );
    if ( columnName != null ) {
      int dotIndex = columnName.indexOf( '.' );
      if ( dotIndex != -1 ) {
        return columnName.substring( dotIndex + 1 );
      }
    }
    return columnName;
  }

  /**
   * Returns a true if values in the column are signed, false if not.
   * <p>
   * This method checks the type of the passed column.  If that type is not numerical, then the result is false. If the
   * type is a numeric then a true is returned.
   *
   * @param column the index of the column to test
   * @return boolean
   * @throws SQLException the sQL exception
   */
  public boolean isSigned( int column ) throws SQLException {
    int numCols = rsmd.getColumnCount();

    if ( column < 1 || column > numCols ) {
      throw new SQLException( "Invalid column value: " + column );
    }

    // we need to convert the thrift type to the SQL type
    int type = rsmd.getColumnType( column );
    switch ( type ) {
      case Types.DOUBLE:
      case Types.DECIMAL:
      case Types.FLOAT:
      case Types.INTEGER:
      case Types.REAL:
      case Types.SMALLINT:
      case Types.TINYINT:
      case Types.BIGINT:
        return true;
    }
    return false;
  }
}
