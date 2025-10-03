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

import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.mapping.MappingFactory;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;

/**
 * Created by bryan on 1/21/16.
 */
public class MappingFactoryImpl implements MappingFactory {
  private final HBaseBytesUtilShim hBaseBytesUtilShim;
  private final HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;

  public MappingFactoryImpl( HBaseBytesUtilShim hBaseBytesUtilShim,
                             HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory ) {
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.hBaseValueMetaInterfaceFactory = hBaseValueMetaInterfaceFactory;
  }

  @Override public Mapping createMapping() {
    return new MappingImpl( new org.pentaho.hadoop.shim.api.internal.hbase.Mapping(), hBaseBytesUtilShim,
      hBaseValueMetaInterfaceFactory );
  }

  @Override public Mapping createMapping( String tableName, String mappingName ) {
    return new MappingImpl( new org.pentaho.hadoop.shim.api.internal.hbase.Mapping( tableName, mappingName ),
      hBaseBytesUtilShim,
      hBaseValueMetaInterfaceFactory );
  }

  @Override
  public Mapping createMapping( String tableName, String mappingName, String keyName, Mapping.KeyType keyType ) {
    org.pentaho.hadoop.shim.api.internal.hbase.Mapping.KeyType type = null;
    if ( keyType != null ) {
      type = org.pentaho.hadoop.shim.api.internal.hbase.Mapping.KeyType.valueOf( keyType.name() );
    }
    return new MappingImpl(
      new org.pentaho.hadoop.shim.api.internal.hbase.Mapping( tableName, mappingName, keyName, type ),
      hBaseBytesUtilShim,
      hBaseValueMetaInterfaceFactory );
  }
}
