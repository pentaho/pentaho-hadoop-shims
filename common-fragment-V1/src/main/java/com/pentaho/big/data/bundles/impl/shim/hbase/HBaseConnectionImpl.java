/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.mapping.MappingFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.table.HBaseTableImpl;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;
import org.pentaho.hadoop.shim.spi.HBaseShim;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 1/21/16.
 */
public class HBaseConnectionImpl implements HBaseConnection {
  private final HBaseConnectionPool hBaseConnectionPool;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public HBaseConnectionImpl( HBaseShim hBaseShim, HBaseBytesUtilShim hBaseBytesUtilShim,
                              Properties connectionProps, LogChannelInterface logChannelInterface,
                              NamedCluster namedCluster ) {
    this( hBaseBytesUtilShim, new HBaseConnectionPool( hBaseShim, connectionProps, logChannelInterface, namedCluster ) );
  }

  public HBaseConnectionImpl( HBaseBytesUtilShim hBaseBytesUtilShim, HBaseConnectionPool hBaseConnectionPool ) {
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.hBaseConnectionPool = hBaseConnectionPool;
  }

  @Override public HBaseTableImpl getTable( String tableName ) throws IOException {
    return new HBaseTableImpl( hBaseConnectionPool, new HBaseValueMetaInterfaceFactoryImpl( this.hBaseBytesUtilShim ),
      hBaseBytesUtilShim, tableName );
  }

  @Override public void checkHBaseAvailable() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().checkHBaseAvailable();
    } catch ( Exception e ) {
      throw IOExceptionUtil.wrapIfNecessary( e );
    }
  }

  @Override public List<String> listTableNames() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().listTableNames();
    } catch ( Exception e ) {
      throw IOExceptionUtil.wrapIfNecessary( e );
    }
  }

  @Override public MappingFactoryImpl getMappingFactory() {
    return new MappingFactoryImpl( hBaseBytesUtilShim, getHBaseValueMetaInterfaceFactory() );
  }

  @Override public HBaseValueMetaInterfaceFactoryImpl getHBaseValueMetaInterfaceFactory() {
    return new HBaseValueMetaInterfaceFactoryImpl( hBaseBytesUtilShim );
  }

  @Override public ByteConversionUtil getByteConversionUtil() {
    return new ByteConversionUtilImpl( hBaseBytesUtilShim );
  }

  @Override public void close() throws IOException {
    hBaseConnectionPool.close();
  }

  @Override public List<String> listNamespaces() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().listNamespaces();
    } catch ( Exception e ) {
      throw IOExceptionUtil.wrapIfNecessary( e );
    }
  }

  @Override public List<String> listTableNamesByNamespace( String namespace ) throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().listTableNamesByNamespace( namespace );
    } catch ( Exception e ) {
      throw IOExceptionUtil.wrapIfNecessary( e );
    }
  }
}
