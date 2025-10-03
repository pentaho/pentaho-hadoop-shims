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
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 2/9/16.
 */
public class MappingFactoryImplTest {
  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private HBaseValueMetaInterfaceFactoryImpl
    hBaseValueMetaInterfaceFactory;
  private MappingFactoryImpl mappingFactory;

  @Before
  public void setup() {
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    hBaseValueMetaInterfaceFactory = mock( HBaseValueMetaInterfaceFactoryImpl.class );
    mappingFactory = new MappingFactoryImpl( hBaseBytesUtilShim, hBaseValueMetaInterfaceFactory );
  }

  @Test
  public void testCreateMapping() {
    assertNotNull( mappingFactory.createMapping() );
  }

  @Test
  public void testCreateMappingTableNameMappingName() {
    String table = "table";
    String mappingName = "mapping";

    Mapping mapping = mappingFactory.createMapping( table, mappingName );

    assertEquals( table, mapping.getTableName() );
    assertEquals( mappingName, mapping.getMappingName() );
  }

  @Test
  public void testCreateMappingTableNameMappingNameKeyNameKeyType() {
    String table = "table";
    String mappingName = "mapping";
    String keyName = "keyName";

    for ( Mapping.KeyType type : Mapping.KeyType.values() ) {
      Mapping mapping = mappingFactory.createMapping( table, mappingName, keyName, type );

      assertEquals( table, mapping.getTableName() );
      assertEquals( mappingName, mapping.getMappingName() );
      assertEquals( keyName, mapping.getKeyName() );
      assertEquals( type, mapping.getKeyType() );
    }
    Mapping mapping = mappingFactory.createMapping( table, mappingName, keyName, null );

    assertEquals( table, mapping.getTableName() );
    assertEquals( mappingName, mapping.getMappingName() );
    assertEquals( keyName, mapping.getKeyName() );
    assertNull( mapping.getKeyType() );
  }
}
