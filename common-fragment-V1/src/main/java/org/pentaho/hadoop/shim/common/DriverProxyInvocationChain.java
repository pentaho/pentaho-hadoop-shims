/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.hadoop.shim.common;

import org.pentaho.hadoop.shim.common.invocationhandler.DriverInvocationHandler;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * DriverProxyInvocationChain is a temporary solution for interacting with Hive drivers. At the time this class was
 * added, many methods from the JDBC API had not yet been implemented and would instead throw SQLExceptions. Also, some
 * methods such as HiveDatabaseMetaData.getTables() did not return any values.  For these reasons, a dynamic proxy chain
 * was put in place, in order to intercept methods that would otherwise not function properly, and instead inject
 * working and/or default functionality.
 * <p>
 * The "chain" part of this class is a result of not having access to all the necessary objects at driver creation time.
 * For this reason, we have to intercept methods that would return such objects, then create and return a proxy to those
 * objects. There are a number of objects and methods to which this applies, so the result is a "chain" of getting
 * access to objects via a proxy, then returning a proxy to those objects, which in turn may return proxied objects for
 * its methods, and so on.
 * <p>
 * The large amount of reflection used here is because not all Hadoop distributions support both Hive and Hive 2. Thus
 * before proxying or anything, we need to make sure we have the classes we need at runtime.
 */
public class DriverProxyInvocationChain {

  public static final String PENTAHO_CURRENT_DBNAME = "pentaho.current.dbname";
  /**
   * The initialized.
   */
  private static boolean initialized = false;

  public static final Date NULL_DATE = new Date( 0 ) {
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
      return "NULL";
    }
  };

  /**
   * The hive1 db meta data class.
   */
  protected static Class<? extends DatabaseMetaData> hive1DbMetaDataClass = null;

  /**
   * The hive2 db meta data class.
   */
  protected static Class<? extends DatabaseMetaData> hive2DbMetaDataClass = null;

  /**
   * The hive1 result set class.
   */
  protected static Class<? extends ResultSet> hive1ResultSetClass = null;

  /**
   * The hive2 result set class.
   */
  protected static Class<? extends ResultSet> hive2ResultSetClass = null;

  /**
   * The hive1 client class.
   */
  protected static Class<?> hive1ClientClass = null;

  /**
   * The hive2 client class.
   */
  protected static Class<?> hive2ClientClass = null;

  /**
   * The hive1 statement class.
   */
  protected static Class<? extends Statement> hive1StatementClass = null;

  /**
   * The hive2 statement class.
   */
  protected static Class<? extends Statement> hive2StatementClass = null;

  public static Class<? extends DatabaseMetaData> getHive1DbMetaDataClass() {
    return hive1DbMetaDataClass;
  }

  public static void setHive1DbMetaDataClass(
    Class<? extends DatabaseMetaData> hive1DbMetaDataClass ) {
    DriverProxyInvocationChain.hive1DbMetaDataClass = hive1DbMetaDataClass;
  }

  public static Class<? extends DatabaseMetaData> getHive2DbMetaDataClass() {
    return hive2DbMetaDataClass;
  }

  public static void setHive2DbMetaDataClass(
    Class<? extends DatabaseMetaData> hive2DbMetaDataClass ) {
    DriverProxyInvocationChain.hive2DbMetaDataClass = hive2DbMetaDataClass;
  }

  public static Class<? extends ResultSet> getHive1ResultSetClass() {
    return hive1ResultSetClass;
  }

  public static void setHive1ResultSetClass( Class<? extends ResultSet> hive1ResultSetClass ) {
    DriverProxyInvocationChain.hive1ResultSetClass = hive1ResultSetClass;
  }

  public static Class<? extends ResultSet> getHive2ResultSetClass() {
    return hive2ResultSetClass;
  }

  public static void setHive2ResultSetClass( Class<? extends ResultSet> hive2ResultSetClass ) {
    DriverProxyInvocationChain.hive2ResultSetClass = hive2ResultSetClass;
  }

  public static Class<?> getHive1ClientClass() {
    return hive1ClientClass;
  }

  public static void setHive1ClientClass( Class<?> hive1ClientClass ) {
    DriverProxyInvocationChain.hive1ClientClass = hive1ClientClass;
  }

  public static Class<?> getHive2ClientClass() {
    return hive2ClientClass;
  }

  public static void setHive2ClientClass( Class<?> hive2ClientClass ) {
    DriverProxyInvocationChain.hive2ClientClass = hive2ClientClass;
  }

  public static Class<? extends Statement> getHive1StatementClass() {
    return hive1StatementClass;
  }

  public static void setHive1StatementClass( Class<? extends Statement> hive1StatementClass ) {
    DriverProxyInvocationChain.hive1StatementClass = hive1StatementClass;
  }

  public static Class<? extends Statement> getHive2StatementClass() {
    return hive2StatementClass;
  }

  public static void setHive2StatementClass( Class<? extends Statement> hive2StatementClass ) {
    DriverProxyInvocationChain.hive2StatementClass = hive2StatementClass;
  }

  protected static ClassLoader driverProxyClassLoader = null;

  /**
   * Gets the proxy.
   *
   * @param intf the intf
   * @param obj  the obj
   * @return the proxy
   */
  public static Driver getProxy( Class<? extends Driver> intf, final Driver obj ) {
    return getProxy( intf, obj, DriverInvocationHandler.class );
  }

  /**
   * Gets the proxy.
   *
   * @param intf the intf
   * @param obj  the obj
   * @return the proxy
   */
  public static Driver getProxy( Class<? extends Driver> intf, final Driver obj, Class driverInvocationHandlerClass ) {
    driverProxyClassLoader = obj.getClass().getClassLoader();
    if ( !initialized ) {
      init();
    }

    InvocationHandler invocationHandler = new DriverInvocationHandler( obj );

    try {
      Constructor driverHandler = driverInvocationHandlerClass.getConstructor( Driver.class );
      invocationHandler = (InvocationHandler) driverHandler.newInstance( Driver.class.cast( obj ) );
    } catch ( NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e ) {
      // ignored
    }
    return (Driver) Proxy.newProxyInstance( driverProxyClassLoader, new Class[] { intf }, invocationHandler );
  }

  /**
   * Initializes the Driver proxy chain
   */
  @SuppressWarnings( { "unchecked" } )
  protected static void init() {

    // Get all the Hive 1 and Hive 2 classes we'll need to call methods on later.

    ClassLoader cl = driverProxyClassLoader;

    try {
      hive1DbMetaDataClass = (Class<? extends DatabaseMetaData>) Class.forName(
        "org.apache.hadoop.hive.jdbc.HiveDatabaseMetaData", false, cl );
      hive1ResultSetClass =
        (Class<? extends ResultSet>) Class.forName( "org.apache.hadoop.hive.jdbc.HiveQueryResultSet", false, cl );
      hive1ClientClass = Class.forName( "org.apache.hadoop.hive.service.HiveInterface", false, cl );
      hive1StatementClass =
        (Class<? extends Statement>) Class.forName( "org.apache.hadoop.hive.jdbc.HiveStatement", false, cl );
    } catch ( ClassNotFoundException cnfe ) {
      //ignored
    }

    try {
      hive2DbMetaDataClass =
        (Class<? extends DatabaseMetaData>) Class.forName( "org.apache.hive.jdbc.HiveDatabaseMetaData", false, cl );
      hive2ResultSetClass =
        (Class<? extends ResultSet>) Class.forName( "org.apache.hive.jdbc.HiveQueryResultSet", false, cl );
      hive2ClientClass = Class.forName( "org.apache.hive.service.cli.thrift.TCLIService$Iface", false, cl );
      hive2StatementClass =
        (Class<? extends Statement>) Class.forName( "org.apache.hive.jdbc.HiveStatement", false, cl );
    } catch ( ClassNotFoundException cnfe ) {
      //ignored
    }

    initialized = true;
  }

  protected static boolean isInitialized() {
    return initialized;
  }

  public static void setInitialized( boolean initialized ) {
    DriverProxyInvocationChain.initialized = initialized;
  }
}
