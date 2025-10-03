/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hbase.shim.common;

import org.apache.hadoop.hbase.util.Bytes;
import org.pentaho.hadoop.shim.common.utils.OverloadedIterator;
import org.pentaho.hadoop.shim.common.utils.OverloadedServiceLoader;
import org.pentaho.hbase.shim.common.wrapper.HBaseConnectionInterface;
import org.pentaho.hbase.shim.spi.IDeserializedBooleanComparator;
import org.pentaho.hbase.shim.spi.IDeserializedNumericComparator;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

public class HBaseConnectionImpl extends CommonHBaseConnection implements HBaseConnectionInterface {

  @Override
  public Class<?> getByteArrayComparableClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hadoop.hbase.filter.ByteArrayComparable" );
  }

  @Override
  public Class<?> getCompressionAlgorithmClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hadoop.hbase.io.compress.Compression$Algorithm" );
  }

  @Override
  public Class<?> getBloomTypeClass() throws ClassNotFoundException {
    return Class.forName( "org.apache.hadoop.hbase.regionserver.BloomType" );
  }

  @Override
  public Class<?> getDeserializedNumericComparatorClass() throws ClassNotFoundException {
    final OverloadedIterator<IDeserializedNumericComparator> providers =
      (OverloadedIterator<IDeserializedNumericComparator>) OverloadedServiceLoader
        .load( IDeserializedNumericComparator.class ).iterator();
    if ( providers.hasNext() ) {
      return providers.next( byte[].class, Bytes.toBytes( 1L ) ).getClass();
    }
    return Class.forName( "org.pentaho.hbase.shim.common.DeserializedNumericComparator" );
  }

  @Override
  public Class<?> getDeserializedBooleanComparatorClass() throws ClassNotFoundException {
    final OverloadedIterator<IDeserializedBooleanComparator> providers =
      (OverloadedIterator<IDeserializedBooleanComparator>) OverloadedServiceLoader
        .load( IDeserializedBooleanComparator.class ).iterator();
    if ( providers.hasNext() ) {
      return providers.next( byte[].class, Bytes.toBytes( true ) ).getClass();
    }
    return Class.forName( "org.pentaho.hbase.shim.common.DeserializedBooleanComparator" );
  }

  protected <T> T doWithContextClassLoader( Callable<T> callable ) throws Exception {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    try {
      return callable.call();
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override
  public void createTable( final String tableName, final List<String> colFamilyNames, final Properties creationProps )
    throws Exception {
    doWithContextClassLoader( new Callable<Void>() {

      @Override
      public Void call() throws Exception {

        HBaseConnectionImpl.super.createTable( tableName, colFamilyNames, creationProps );
        return null;
      }
    } );
  }

  @Override
  public void deleteTable( final String tableName ) throws Exception {
    doWithContextClassLoader( new Callable<Void>() {

      @Override
      public Void call() throws Exception {

        HBaseConnectionImpl.super.deleteTable( tableName );
        return null;
      }
    } );
  }

  @Override
  public void disableTable( final String tableName ) throws Exception {
    doWithContextClassLoader( new Callable<Void>() {

      @Override
      public Void call() throws Exception {

        HBaseConnectionImpl.super.disableTable( tableName );
        return null;
      }
    } );
  }

  @Override
  public void enableTable( final String tableName ) throws Exception {
    doWithContextClassLoader( new Callable<Void>() {

      @Override
      public Void call() throws Exception {

        HBaseConnectionImpl.super.enableTable( tableName );
        return null;
      }
    } );
  }

  @Override
  public List<String> getTableFamiles( final String tableName ) throws Exception {
    return doWithContextClassLoader( new Callable<List<String>>() {

      @Override
      public List<String> call() throws Exception {
        return HBaseConnectionImpl.super.getTableFamiles( tableName );
      }
    } );
  }

  @Override
  public boolean isTableAvailable( final String tableName ) throws Exception {
    return doWithContextClassLoader( new Callable<Boolean>() {

      @Override
      public Boolean call() throws Exception {
        return HBaseConnectionImpl.super.isTableAvailable( tableName );
      }
    } );
  }

  @Override
  public boolean isTableDisabled( final String tableName ) throws Exception {
    return doWithContextClassLoader( new Callable<Boolean>() {

      @Override
      public Boolean call() throws Exception {
        return HBaseConnectionImpl.super.isTableDisabled( tableName );
      }
    } );
  }

  @Override
  public List<String> listTableNames() throws Exception {
    return doWithContextClassLoader( new Callable<List<String>>() {

      @Override
      public List<String> call() throws Exception {
        return HBaseConnectionImpl.super.listTableNames();
      }
    } );
  }

  @Override
  public boolean tableExists( final String tableName ) throws Exception {
    return doWithContextClassLoader( new Callable<Boolean>() {

      @Override
      public Boolean call() throws Exception {
        return HBaseConnectionImpl.super.tableExists( tableName );
      }
    } );
  }

  @Override public List<String> listNamespaces() throws Exception {
    return doWithContextClassLoader( new Callable<List<String>>() {

      @Override
      public List<String> call() throws Exception {
        return HBaseConnectionImpl.super.listNamespaces();
      }
    } );
  }

  @Override public List<String> listTableNamesByNamespace( String namespace ) throws Exception {
    return doWithContextClassLoader( new Callable<List<String>>() {

      @Override
      public List<String> call() throws Exception {
        return HBaseConnectionImpl.super.listTableNamesByNamespace( namespace );
      }
    } );
  }

}
