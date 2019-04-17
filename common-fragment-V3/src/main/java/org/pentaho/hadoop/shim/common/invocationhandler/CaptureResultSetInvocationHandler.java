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
 * User: Dzmitry Stsiapanau Date: 01/17/2017 Time: 15:17
 */


import org.pentaho.hadoop.shim.common.DriverProxyInvocationChain;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/**
 * CaptureResultSetInvocationHandler is a generic proxy handler class for any java.sql.* class that has methods to
 * return ResultSet objects. However the code in this file is specifically for handling Hive JDBC calls, and therefore
 * should not be used to proxy any other JDBC objects besides those provided by Hive.
 *
 * @param <T> the generic type of object whose methods return ResultSet objects
 */
public class CaptureResultSetInvocationHandler<T extends Statement> implements InvocationHandler {
  /**
   * The object whose methods return ResultSet objects.
   */
  T t;

  /**
   * Instantiates a new capture result set invocation handler.
   *
   * @param t the t
   */
  public CaptureResultSetInvocationHandler( T t ) {
    this.t = t;
  }

  /**
   * Intercepts methods called on the object to possibly perform alternate processing.
   *
   * @param proxy  the proxy
   * @param method the method
   * @param args   the args
   * @return the object
   * @throws Throwable the throwable
   */
  @Override
  public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable {
    // try to invoke the method as-is
    String methodName = method.getName();
    try {
      final boolean isSetTimestamp = "setTimestamp".equals( methodName );
      // We want to intercept all setTimestamp and date calls to set them as a string instead,
      // Causing hive driver to put single quotes around them
      // Exception to this is the NULL_DATE date which signifies that we're explicitly
      // Trying to get NULL into the paramter map without quotes around it
      if ( PreparedStatement.class.isInstance( proxy ) && ( isSetTimestamp || "setDate".equals( methodName ) )
        && args[ 1 ] != DriverProxyInvocationChain.NULL_DATE ) {
        PreparedStatement ps = (PreparedStatement) proxy;
        if ( args[ 1 ] == null ) {
          ps.setNull( (Integer) args[ 0 ], isSetTimestamp ? Types.TIMESTAMP : Types.DATE );
        } else {
          final String value;
          if ( args.length == 3
            && java.util.Date.class.isAssignableFrom( method.getParameterTypes()[ 1 ] )
            && Calendar.class.isAssignableFrom( method.getParameterTypes()[ 2 ] ) ) {
            final Calendar calendar;
            if ( args[ 2 ] == null ) {
              calendar = Calendar.getInstance();
            } else {
              calendar = Calendar.getInstance( ( (Calendar) args[ 2 ] ).getTimeZone() );
            }
            calendar.setTime( (java.util.Date) args[ 1 ] );
            if ( isSetTimestamp ) {
              value = new Timestamp( calendar.getTimeInMillis() ).toString();
            } else {
              value = new Date( calendar.getTimeInMillis() ).toString();
            }
          } else {
            value = args[ 1 ].toString();
          }
          ps.setString( (Integer) args[ 0 ], value );
        }
        return null;
      } else {
        return getProxiedObject( method.invoke( t, args ) );
      }
    } catch ( InvocationTargetException ite ) {
      Throwable cause = ite.getCause();

      if ( cause instanceof SQLException ) {
        if ( cause.getMessage().equals( "Method not supported" ) ) {
          // Intercept PreparedStatement.getMetaData() to see if it throws an exception
          if ( "getMetaData".equals( methodName ) && ( args == null || args.length == 0 ) ) {
            return getProxiedObject( getMetaData() );
          } else if ( PreparedStatement.class.isInstance( proxy ) ) {
            PreparedStatement ps = (PreparedStatement) proxy;
            if ( "setObject".equals( methodName ) && args.length == 2 && Integer.class.isInstance( args[ 0 ] ) ) {
              // Intercept PreparedStatement.setObject(position, value)
              // Set value using value type instead
              // This should already be fixed in later Hive JDBC versions:

              int parameterIndex = (Integer) args[ 0 ];
              Object x = args[ 1 ];

              if ( x == null ) {
                // PreparedStatement.setNull may not be supported
                ps.setNull( parameterIndex, Types.NULL );
              } else if ( x instanceof String ) {
                ps.setString( parameterIndex, (String) x );
              } else if ( x instanceof Short ) {
                ps.setShort( parameterIndex, ( (Short) x ).shortValue() );
              } else if ( x instanceof Integer ) {
                ps.setInt( parameterIndex, ( (Integer) x ).intValue() );
              } else if ( x instanceof Long ) {
                ps.setLong( parameterIndex, ( (Long) x ).longValue() );
              } else if ( x instanceof Float ) {
                ps.setFloat( parameterIndex, ( (Float) x ).floatValue() );
              } else if ( x instanceof Double ) {
                ps.setDouble( parameterIndex, ( (Double) x ).doubleValue() );
              } else if ( x instanceof Boolean ) {
                ps.setBoolean( parameterIndex, ( (Boolean) x ).booleanValue() );
              } else if ( x instanceof Byte ) {
                ps.setByte( parameterIndex, ( (Byte) x ).byteValue() );
              } else if ( x instanceof Character ) {
                ps.setString( parameterIndex, x.toString() );
              } else {
                // Can't infer a type.
                throw new SQLException( "Type " + x.getClass() + " is not yet supported", cause );
              }
              return null;
            } else if ( "setNull".equals( methodName )
              && args.length == 2 && Integer.class.isInstance( args[ 0 ] ) ) {

              int parameterIndex = (Integer) args[ 0 ];
              // Overriding date to get NULL into query with no quotes around it
              ps.setDate( parameterIndex, DriverProxyInvocationChain.NULL_DATE );

              return null;
            }
          }
        }
      }
      throw cause;
    }
  }

  /**
   * Returns the result set meta data.  If a result set was not created by running an execute or executeQuery then a
   * null is returned.
   *
   * @return null is returned if the result set is null
   * @throws SQLException if an error occurs while getting metadata
   * @see java.sql.PreparedStatement#getMetaData()
   */

  public ResultSetMetaData getMetaData() {
    ResultSetMetaData rsmd = null;
    if ( t instanceof Statement ) {
      try {
        ResultSet resultSet = ( (Statement) t ).getResultSet();
        rsmd = ( resultSet == null ? null : resultSet.getMetaData() );
      } catch ( SQLException se ) {
        rsmd = null;
      }
    }
    return rsmd;
  }

  private Object getProxiedObject( Object o ) {
    if ( o == null ) {
      return null;
    }

    if ( o instanceof ResultSet ) {
      ResultSet r = (ResultSet) o;


      return (ResultSet) Proxy.newProxyInstance( r.getClass().getClassLoader(),
        new Class[] { ResultSet.class }, new ResultSetInvocationHandler( r, t ) );
    } else if ( o instanceof ResultSetMetaData ) {
      ResultSetMetaData r = (ResultSetMetaData) o;

      return (ResultSetMetaData) Proxy.newProxyInstance( r.getClass().getClassLoader(),
        new Class[] { ResultSetMetaData.class }, new ResultSetMetaDataInvocationHandler( r ) );
    } else {
      return o;
    }
  }
}
