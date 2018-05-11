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
 * User: Dzmitry Stsiapanau Date: 01/17/2017 Time: 15:16
 */

import org.pentaho.hadoop.shim.common.DriverProxyInvocationChain;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * DatabaseMetaDataInvocationHandler is a proxy handler class for java.sql.DatabaseMetaData. However the code in this
 * file is specifically for handling Hive JDBC calls, and therefore should not be used to proxy any other JDBC objects
 * besides those provided by Hive.
 */
public class DatabaseMetaDataInvocationHandler implements InvocationHandler {

  /**
   * The "real" database metadata object.
   */
  DatabaseMetaData t;

  /**
   * The connection proxy associated with the DatabaseMetaData object
   */
  ConnectionInvocationHandler c;

  /**
   * Instantiates a new database meta data invocation handler.
   *
   * @param t the database metadata object to proxy
   */
  public DatabaseMetaDataInvocationHandler( DatabaseMetaData t, ConnectionInvocationHandler c ) {
    this.t = t;
    this.c = c;
  }

  /**
   * Intercepts methods called on the DatabaseMetaData object to possibly perform alternate processing.
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
      if ( "getTables".equals( methodName ) ) {

        // For Hive/Impala drivers, we need to intercept the getTables method even though it doesn't
        // throw an exception, because the ResultSet is empty. The temp fix is to try an execute a
        // HiveQL query of "show tables". This only returns one (differently-named) column containing
        // the table/view name, vs. getTables() which returns much metadata. C'est la vie.

        if ( DriverProxyInvocationChain.getHive1DbMetaDataClass() != null && DriverProxyInvocationChain
          .getHive1DbMetaDataClass().isAssignableFrom( t.getClass() ) ) {
          return getTables( t, DriverProxyInvocationChain.getHive1DbMetaDataClass(),
            DriverProxyInvocationChain.getHive1StatementClass(), DriverProxyInvocationChain.getHive1ClientClass(),
            (String) args[ 0 ], (String) args[ 1 ], (String) args[ 2 ], (String[]) args[ 3 ], method, args );
        }
        if ( DriverProxyInvocationChain.getHive2DbMetaDataClass() != null && DriverProxyInvocationChain
          .getHive2DbMetaDataClass().isAssignableFrom( t.getClass() ) ) {
          return getTables( t, DriverProxyInvocationChain.getHive2DbMetaDataClass(),
            DriverProxyInvocationChain.getHive2StatementClass(), DriverProxyInvocationChain.getHive2ClientClass(),
            (String) args[ 0 ], (String) args[ 1 ], (String) args[ 2 ], (String[]) args[ 3 ], method, args );
        }
      } else if ( "getConnection".equals( methodName ) ) {
        // Return the connection
        return c;
      } else if ( "getIdentifierQuoteString".equals( methodName ) ) {
        // Need to intercept getIdentifierQuoteString() before trying the driver version, as our "fixed"
        // drivers return a single quote when it should be empty.
        return getIdentifierQuoteString();
      }

      // try to invoke the method as-is
      Object o = method.invoke( t, args );
      if ( o instanceof ResultSet ) {
        ResultSet r = (ResultSet) o;

        return (ResultSet) Proxy.newProxyInstance( r.getClass().getClassLoader(),
          new Class[] { ResultSet.class }, new ResultSetInvocationHandler( r ) );
      } else {
        return o;
      }
    } catch ( Throwable t ) {
      if ( t instanceof InvocationTargetException ) {
        Throwable cause = t.getCause();
        throw cause;
      } else {
        throw t;
      }
    }
  }

  /**
   * Returns the identifier quote string.  This is HiveQL specific
   *
   * @return String the quote string for identifiers in HiveQL
   * @throws SQLException if any SQL error occurs
   */
  public String getIdentifierQuoteString() throws SQLException {
    return "";
  }

  /**
   * Gets the tables for the specified database.
   *
   * @param originalObject   the original object
   * @param dbMetadataClass  the db metadata class
   * @param statementClass   the statement class
   * @param clientClass      the client class
   * @param catalog          the catalog
   * @param schemaPattern    the schema pattern
   * @param tableNamePattern the table name pattern
   * @param types            the types
   * @param method           the original method
   * @param args             the original args
   * @return the tables
   * @throws Exception the exception
   */
  public ResultSet getTables( Object originalObject, Class<? extends DatabaseMetaData> dbMetadataClass,
                              Class<? extends Statement> statementClass, Class<?> clientClass,
                              String catalog, String schemaPattern,
                              String tableNamePattern, String[] types, Method method, Object[] args )
    throws Exception {

    boolean tables = false;
    if ( types == null ) {
      tables = true;
    } else {
      for ( String type : types ) {
        if ( "TABLE".equals( type ) ) {
          tables = true;
        }
      }
    }

    // If we're looking for tables, execute "show tables" query instead
    if ( tables ) {
      try {
        // try to invoke the method as-is
        Object o = method.invoke( originalObject, args );
        if ( o instanceof ResultSet ) {
          ResultSet r = (ResultSet) o;
          ResultSet ret = (ResultSet) Proxy.newProxyInstance( r.getClass().getClassLoader(),
            new Class[] { ResultSet.class }, new ResultSetInvocationHandler( r ) );
          if ( ret.isBeforeFirst() ) {
            return ret;
          }
        }
      } catch ( Exception e ) {
        // ignored
      }
      Statement showTables = null;

      // If we have a valid Connection, create and proxy the show tables statement
      if ( c != null ) {
        Statement st = c.createStatement( c.connection, null );
        showTables = (Statement) Proxy.newProxyInstance( st.getClass().getClassLoader(),
          new Class[] { Statement.class }, new CaptureResultSetInvocationHandler<Statement>( st ) );
      } else {
        Object client;
        Constructor<? extends Statement> hiveStatementCtor =
          (Constructor<? extends Statement>) statementClass.getDeclaredConstructor( clientClass );

        // Try reflection and private member access first
        try {
          Field clientField = dbMetadataClass.getDeclaredField( "client" );
          client = clientField.get( originalObject );
          showTables = hiveStatementCtor.newInstance( clientClass.cast( client ) );
        } catch ( Exception e ) {
          showTables = null;
        }

        if ( showTables == null ) {
          try {
            Method getClient = dbMetadataClass.getDeclaredMethod( "getClient" );
            client = getClient.invoke( originalObject );
            showTables = hiveStatementCtor.newInstance( clientClass.cast( client ) );
          } catch ( Exception e ) {
            showTables = null;
          }
        }
      }
      // If we found a way to call "show tables", do it
      if ( showTables != null ) {
        ResultSet rs;
        if ( schemaPattern != null ) {
          rs = showTables.executeQuery( String.format( "show tables in %s", schemaPattern ) );
        } else {
          rs = showTables.executeQuery( "show tables" );
        }
        if ( rs != null ) {
          return (ResultSet) Proxy.newProxyInstance( rs.getClass().getClassLoader(),
            new Class[] { ResultSet.class }, new ResultSetInvocationHandler( rs ) );
        } else {
          return null;
        }
      } else {
        throw new Exception( "Cannot execute SHOW TABLES query" );
      }
    } else {
      Method getTables =
        dbMetadataClass.getDeclaredMethod( "getTables", String.class, String.class, String.class, String[].class );
      ResultSet rs = (ResultSet) getTables.invoke( originalObject, catalog, schemaPattern, tableNamePattern, types );
      return rs;
    }
  }
}
