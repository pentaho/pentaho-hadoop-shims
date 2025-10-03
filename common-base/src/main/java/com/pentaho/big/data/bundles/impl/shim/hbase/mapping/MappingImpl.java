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


package com.pentaho.big.data.bundles.impl.shim.hbase.mapping;

import com.google.common.collect.Maps;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseValueMeta;
import org.w3c.dom.Node;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bryan on 1/21/16.
 */
public class MappingImpl implements Mapping {
  private final org.pentaho.hadoop.shim.api.internal.hbase.Mapping delegate;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;
  private final HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;

  public MappingImpl( org.pentaho.hadoop.shim.api.internal.hbase.Mapping delegate,
                      HBaseBytesUtilShim hBaseBytesUtilShim,
                      HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory ) {
    this.delegate = delegate;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.hBaseValueMetaInterfaceFactory = hBaseValueMetaInterfaceFactory;
  }

  @Override public String addMappedColumn( HBaseValueMetaInterface column, boolean isTupleColumn ) throws Exception {
    if ( column instanceof HBaseValueMeta ) {
      return delegate.addMappedColumn( (HBaseValueMeta) column, isTupleColumn );
    }
    return delegate.addMappedColumn( hBaseValueMetaInterfaceFactory.copy( column ), isTupleColumn );
  }

  @Override public String getTableName() {
    return delegate.getTableName();
  }

  @Override public void setTableName( String tableName ) {
    delegate.setTableName( tableName );
  }

  @Override public String getMappingName() {
    return delegate.getMappingName();
  }

  @Override public void setMappingName( String mappingName ) {
    delegate.setMappingName( mappingName );
  }

  @Override public String getKeyName() {
    return delegate.getKeyName();
  }

  @Override public void setKeyName( String keyName ) {
    delegate.setKeyName( keyName );
  }

  @Override public void setKeyTypeAsString( String type ) throws Exception {
    delegate.setKeyTypeAsString( type );
  }

  @Override public KeyType getKeyType() {
    org.pentaho.hadoop.shim.api.internal.hbase.Mapping.KeyType keyType = delegate.getKeyType();
    if ( keyType == null ) {
      return null;
    }
    return KeyType.valueOf( keyType.name() );
  }

  @Override public void setKeyType( KeyType type ) {
    if ( type == null ) {
      delegate.setKeyType( null );
    } else {
      delegate.setKeyType( org.pentaho.hadoop.shim.api.internal.hbase.Mapping.KeyType.valueOf( type.name() ) );
    }
  }

  @Override public boolean isTupleMapping() {
    return delegate.isTupleMapping();
  }

  @Override public void setTupleMapping( boolean t ) {
    delegate.setTupleMapping( t );
  }

  @Override public String getTupleFamilies() {
    return delegate.getTupleFamilies();
  }

  @Override public void setTupleFamilies( String f ) {
    delegate.setTupleFamilies( f );
  }

  @Override public int numMappedColumns() {
    return delegate.getMappedColumns().size();
  }

  @Override public String[] getTupleFamiliesSplit() {
    return getTupleFamilies().split( HBaseValueMeta.SEPARATOR );
  }

  @Override public Map<String, HBaseValueMetaInterface> getMappedColumns() {
    return Collections.unmodifiableMap( Maps.transformEntries( delegate.getMappedColumns(),
      new Maps.EntryTransformer<String, HBaseValueMeta, HBaseValueMetaInterface>() {
        @Override
        public HBaseValueMetaInterface transformEntry( String key, HBaseValueMeta value ) {
          if ( value instanceof HBaseValueMetaInterface ) {
            return (HBaseValueMetaInterface) value;
          }
          return hBaseValueMetaInterfaceFactory.copy( value );
        }
      } ) );
  }

  @Override public void setMappedColumns( Map<String, HBaseValueMetaInterface> cols ) {
    delegate.setMappedColumns( new HashMap<String, HBaseValueMeta>( Maps.transformEntries( cols,
      new Maps.EntryTransformer<String, HBaseValueMetaInterface, HBaseValueMeta>() {
        @Override
        public HBaseValueMeta transformEntry( String key, HBaseValueMetaInterface value ) {
          if ( value instanceof HBaseValueMeta ) {
            return (HBaseValueMeta) value;
          }
          return hBaseValueMetaInterfaceFactory.copy( value );
        }
      } ) ) );
  }

  @Override public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    delegate.saveRep( rep, id_transformation, id_step );
  }

  @Override public String getXML() {
    return delegate.getXML();
  }

  @Override public boolean loadXML( Node stepnode ) throws KettleXMLException {
    return delegate.loadXML( stepnode );
  }

  @Override public boolean readRep( Repository rep, ObjectId id_step ) throws KettleException {
    return delegate.readRep( rep, id_step );
  }

  @Override public String getFriendlyName() {
    return delegate.getMappingName() + HBaseValueMeta.SEPARATOR + delegate.getTableName();
  }

  @Override public Object decodeKeyValue( byte[] rawval ) throws KettleException {
    return HBaseValueMeta.decodeKeyValue( rawval, delegate, hBaseBytesUtilShim );
  }

  @Override public String toString() {
    return delegate.toString();
  }
}
