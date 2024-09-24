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
package org.pentaho.hadoop.shim.common.invocationhandler;

/**
 * User: Dzmitry Stsiapanau Date: 01/17/2017 Time: 15:14
 */

import org.pentaho.hadoop.shim.common.HiveSQLUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DriverInvocationHandler is a proxy handler class for java.sql.Driver. However the code in this file is specifically
 * for handling Hive JDBC calls, and therefore should not be used to proxy any other JDBC objects besides those provided
 * by Hive.
 */
public class DriverInvocationHandler implements InvocationHandler {

  /**
   * The driver.
   */
  Driver driver;

  /**
   * Instantiates a new Driver proxy handler.
   *
   * @param obj the Driver to proxy
   */
  public DriverInvocationHandler( Driver obj ) {
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

