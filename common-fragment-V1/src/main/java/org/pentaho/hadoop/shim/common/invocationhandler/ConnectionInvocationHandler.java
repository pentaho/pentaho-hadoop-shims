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
 * User: Dzmitry Stsiapanau Date: 01/17/2017 Time: 15:15
 */

import org.pentaho.hadoop.shim.common.HiveSQLUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * ConnectionInvocationHandler is a proxy handler class for java.sql.Connection. However the code in this file is
 * specifically for handling Hive JDBC calls, and therefore should not be used to proxy any other JDBC objects besides
 * those provided by Hive.
 */
public class ConnectionInvocationHandler implements InvocationHandler {

  /**
   * The "real" connection.
   */
  Connection connection;

  /**
   * Instantiates a new connection invocation handler.
   *
   * @param obj the obj
   */
  public ConnectionInvocationHandler( Connection obj ) {
    connection = obj;
  }

  /**
   * Intercepts methods called on the Connection to possibly perform alternate processing.
   *
   * @param proxy  the proxy
   * @param method the method
   * @param args   the args
   * @return the object
   * @throws Throwable the throwable
   */
  @Override
  public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable {
    Object o = null;
    try {
      if ( "prepareStatement".equals( method.getName() ) ) {
        String sql = (String) args[ 0 ];
        args[ 0 ] = HiveSQLUtils.processSQLString( sql );
      }
      o = method.invoke( connection, args );
    } catch ( Throwable t ) {

      if ( t instanceof InvocationTargetException ) {
        Throwable cause = t.getCause();

        if ( cause instanceof SQLException ) {
          String methodName = method.getName();
          if ( cause.getMessage().startsWith( "Method not supported" )
            || cause.getMessage().equals( "enabling autocommit is not supported" ) ) {
            if ( "createStatement".equals( methodName ) ) {
              o = createStatement( connection, args );
            } else if ( "isReadOnly".equals( methodName ) ) {
              o = Boolean.FALSE;
            } else if ( "setReadOnly".equals( methodName ) ) {
              o = (Void) null;
            } else if ( "setAutoCommit".equals( methodName ) ) {
              o = (Void) null;
            } else {
              throw cause;
            }
          } else {
            throw cause;
          }
        } else {
          throw cause;
        }
      } else {
        throw t;
      }

    }
    if ( o instanceof DatabaseMetaData ) {
      DatabaseMetaData dbmd = (DatabaseMetaData) o;

      // Intercept the DatabaseMetaData object so we can proxy that too
      return (DatabaseMetaData) Proxy.newProxyInstance( dbmd.getClass().getClassLoader(),
        new Class[] { DatabaseMetaData.class }, new DatabaseMetaDataInvocationHandler( dbmd, this ) );
    } else if ( o instanceof PreparedStatement ) {
      PreparedStatement st = (PreparedStatement) o;

      // Intercept the Statement object so we can proxy that too
      return (PreparedStatement) Proxy.newProxyInstance( st.getClass().getClassLoader(),
        new Class[] { PreparedStatement.class }, new CaptureResultSetInvocationHandler<PreparedStatement>( st ) );
    } else if ( o instanceof Statement ) {
      Statement st = (Statement) o;

      // Intercept the Statement object so we can proxy that too
      return (Statement) Proxy.newProxyInstance( st.getClass().getClassLoader(),
        new Class[] { Statement.class }, new CaptureResultSetInvocationHandler<Statement>( st ) );
    } else {
      return o;
    }
  }

  /**
   * Creates a statement for the given Connection with the specified arguments
   *
   * @param c    the connection object
   * @param args the arguments
   * @return the statement
   * @throws SQLException the sQL exception
   * @see java.sql.Connection#createStatement(int, int)
   */
  public Statement createStatement( Connection c, Object[] args ) throws SQLException {
    if ( c.isClosed() ) {
      throw new SQLException( "Can't create Statement, connection is closed " );
    }
      /* Ignore these for now -- this proxy stuff should go away anyway when the fixes are made to Apache Hive

      int resultSetType = (Integer)args[0];
      int resultSetConcurrency = (Integer)args[1];

      if(resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
        throw new SQLException(
            "Invalid parameter to createStatement() only TYPE_FORWARD_ONLY is supported
            ("+resultSetType+"!="+ResultSet.TYPE_FORWARD_ONLY+")");
      }

      if(resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
        throw new SQLException(
            "Invalid parameter to createStatement() only CONCUR_READ_ONLY is supported");
      }*/
    return c.createStatement();
  }
}
