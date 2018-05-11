/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.hadoop.shim.common.invocationhandler;

/**
 * User: Dzmitry Stsiapanau Date: 01/17/2017 Time: 15:18
 */

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * ResultSetInvocationHandler is a proxy handler class for java.sql.ResultSet. However the code in this file is
 * specifically for handling Hive JDBC calls, and therefore should not be used to proxy any other JDBC objects besides
 * those provided by Hive.
 */
public class ResultSetInvocationHandler implements InvocationHandler {

  /**
   * The "real" ResultSet object .
   */
  ResultSet rs;
  Statement st;

  /**
   * Instantiates a new result set invocation handler.
   *
   * @param r the r
   */
  public ResultSetInvocationHandler( ResultSet r ) {
    rs = r;
  }

  /**
   * Instantiates a new result set invocation handler.
   *
   * @param r the r
   */
  public ResultSetInvocationHandler( ResultSet r, Statement s ) {
    rs = r;
    st = s;
  }

  /**
   * Intercepts methods called on the ResultSet to possibly perform alternate processing.
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

      // Intercept the getString(String) method to implement the hack for "show tables" vs. getTables()
      if ( "getString".equals( methodName ) && args != null && args.length == 1 && args[ 0 ] instanceof String ) {
        return getString( (String) args[ 0 ] );
      } else if ( "getType".equals( methodName ) ) {
        // Return TYPE_FORWARD_ONLY (scrollability is not really supported)
        return ResultSet.TYPE_FORWARD_ONLY;
      } else {
        Object o = method.invoke( rs, args );

        if ( o instanceof ResultSetMetaData ) {
          // Intercept the ResultSetMetaData object so we can proxy that too
          return (ResultSetMetaData) Proxy.newProxyInstance( o.getClass().getClassLoader(),
            new Class[] { ResultSetMetaData.class },
            new ResultSetMetaDataInvocationHandler( (ResultSetMetaData) o ) );
        } else {
          return o;
        }
      }
    } catch ( Throwable t ) {

      if ( t instanceof InvocationTargetException ) {
        Throwable cause = t.getCause();
        String methodName = method.getName();

        if ( cause instanceof SQLException ) {
          if ( cause.getMessage().equals( "Method not supported" ) ) {
            if ( "getStatement".equals( methodName ) ) {
              return getStatement();
            } else {
              throw cause;
            }
          } else {
            throw cause;
          }
        } else if ( cause instanceof IllegalMonitorStateException && "close".equals( methodName ) ) {
          // Workaround for BISERVER-11782. By this moment invocation of closeClientOperation did it's job and failed
          // trying to unlock not locked lock, just ignore this.
          return null;
        } else {
          throw cause;
        }
      } else {
        throw t;
      }
    }
  }

  private Statement getStatement() {
    return st;
  }

  /**
   * Gets the string value from the current row at the column with the specified name.
   *
   * @param columnName the column name
   * @return the string value of the row at the column with the specified name
   * @throws SQLException if the column name cannot be found
   */
  public String getString( String columnName ) throws SQLException {

    String columnVal = null;
    SQLException exception = null;
    try {
      columnVal = rs.getString( columnName );
    } catch ( SQLException se ) {
      // Save for returning later
      exception = se;
    }
    if ( columnVal != null ) {
      return columnVal;
    }
    if ( columnName != null && "TABLE_NAME".equals( columnName ) ) {
      if ( columnName != null && "TABLE_NAME".equals( columnName ) ) {
        try {
          // If we're using the "show tables" hack in getTables(), return the first column
          columnVal = rs.getString( 1 );
        } catch ( SQLException se ) {
          throw ( exception == null ) ? se : exception;
        }
      }
    }
    return columnVal;
  }
}