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

package com.pentaho.big.data.bundles.impl.shim.hbase.meta;


import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseValueMeta;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;

public class HBaseValueMetaInterfaceFactoryImplTest {
  HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;

  @Before
  public void setUp() throws Exception {
    HBaseBytesUtilShim hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    hBaseValueMetaInterfaceFactory = new HBaseValueMetaInterfaceFactoryImpl( hBaseBytesUtilShim );
  }

  @Test
  public void testCopyIsLongOrDoubleType() throws Exception {
    HBaseValueMeta hBaseLongValueMeta = new HBaseValueMeta( "col_family_1,col_name_1,long", 5, 0, 0 );
    hBaseLongValueMeta.setIsLongOrDouble( true );
    HBaseValueMeta hBaseIntegerValueMeta = new HBaseValueMeta( "col_family_2,col_name_2,integer", 5, 0, 0 );
    hBaseIntegerValueMeta.setIsLongOrDouble( false );
    HBaseValueMeta hBaseDoubleValueMeta = new HBaseValueMeta( "col_family_3,col_name_3,double", 2, 0, 0 );
    hBaseDoubleValueMeta.setIsLongOrDouble( true );
    HBaseValueMeta hBaseFloatValueMeta = new HBaseValueMeta( "col_family_4,col_name_4,float", 2, 0, 0 );
    hBaseFloatValueMeta.setIsLongOrDouble( false );

    List<HBaseValueMeta> metaToCopyList = new ArrayList<>();
    metaToCopyList.add( hBaseLongValueMeta );
    metaToCopyList.add( hBaseLongValueMeta );
    metaToCopyList.add( hBaseDoubleValueMeta );
    metaToCopyList.add( hBaseFloatValueMeta );

    HBaseValueMetaInterfaceImpl metaAfterCopy;
    for ( HBaseValueMeta metaToCopy : metaToCopyList ) {
      metaAfterCopy = hBaseValueMetaInterfaceFactory.copy( metaToCopy );
      assertEquals( metaToCopy.getType(), metaAfterCopy.getType() );
      assertEquals( metaToCopy.getIsLongOrDouble(), metaToCopy.getIsLongOrDouble() );
      assertEquals( metaToCopy.getHBaseTypeDesc(), metaAfterCopy.getHBaseTypeDesc() );
    }
  }


}
