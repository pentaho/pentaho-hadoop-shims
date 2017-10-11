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
package org.pentaho.hadoop.shim.hdi35.invocationhandler;

/**
 * User: Dzmitry Stsiapanau Date: 01/17/2017 Time: 15:14
 */

import org.pentaho.hadoop.shim.common.HiveSQLUtils;
import org.pentaho.hadoop.shim.common.invocationhandler.ConnectionInvocationHandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * DriverInvocationHandler is a proxy handler class for java.sql.Driver. However the code in this file is specifically
 * for handling Hive JDBC calls, and therefore should not be used to proxy any other JDBC objects besides those provided
 * by Hive.
 */
public class HDIDriverInvocationHandler implements InvocationHandler {

  /**
   * The driver.
   */
  Driver driver;

  /**
   * Instantiates a new Driver proxy handler.
   *
   * @param obj the Driver to proxy
   */
  public HDIDriverInvocationHandler( Driver obj ) {
    driver = obj;
  }

  /**
   * Intercepts methods called on the Driver to possibly perform alternate processing.
   *
   * @param proxy  the proxy object
   * @param method the method being invoked
   * @param args   the arguments to the method
   * @return the object returned by whatever processing takes place
   * @throws Throwable if an error occurs during processing
   */
  @Override
  public Object invoke( final Object proxy, Method method, Object[] args ) throws Throwable {

    try {
      if ( "connect".equals( method.getName() ) || "acceptsURL".equals( method.getName() ) || "getPropertyInfo"
        .equals( method.getName() ) ) {
        String sql = (String) args[ 0 ];
        if ( !sql.contains( "transportMode" ) ) {
          args[ 0 ] = sql + ";httpPath=cliservice;transportMode=http";
        }
        if ( !"acceptsURL".equals( method.getName() ) ) {
          Properties info = (Properties) args[ 1 ];
          info.setProperty( "httpPath", "cliservice" );
          info.setProperty( "transportMode", "http" );
        }
      }

      Object o = method.invoke( driver, args );
      if ( o instanceof Connection ) {
        // Intercept the Connection object so we can proxy that too
        Connection proxiedConnection = (Connection) Proxy.newProxyInstance( o.getClass().getClassLoader(),
          new Class[] { Connection.class }, new ConnectionInvocationHandler( (Connection) o ) );

        String dbName = HiveSQLUtils.getDatabaseNameFromURL( (String) args[ 0 ] );
        useSchema( dbName, proxiedConnection.createStatement() );

        return proxiedConnection;
      } else {
        return o;
      }
    } catch ( Throwable t ) {
      throw ( t instanceof InvocationTargetException ) ? t.getCause() : t;
    }
  }

  protected static void useSchema( String dbName, Statement statement ) throws SQLException {
    if ( dbName.trim().length() > 0 ) {
      String queries = String.format( "use %s", dbName );
      statement.execute( queries );
    }
  }
}

