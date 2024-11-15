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


package org.pentaho.bigdata.api.hdfs.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/3/15.
 */
public class HadoopFileSystemLocatorImplTest {
  private List<HadoopFileSystemFactory> hadoopFileSystemFactories;
  private HadoopFileSystemLocatorImpl hadoopFileSystemLocator;
  private HadoopFileSystemFactory hadoopFileSystemFactory;
  private HadoopFileSystem hadoopFileSystem;
  private NamedCluster namedCluster;

  @Before
  public void setup() {
    namedCluster = mock( NamedCluster.class );
    hadoopFileSystem = mock( HadoopFileSystem.class );
    hadoopFileSystemFactories = new ArrayList<>();
    hadoopFileSystemFactory = mock( HadoopFileSystemFactory.class );
    hadoopFileSystemFactories.add( hadoopFileSystemFactory );
    hadoopFileSystemLocator = new HadoopFileSystemLocatorImpl( hadoopFileSystemFactories );
  }

  @Test
  public void testIOException() throws IOException, ClusterInitializationException {
    when( hadoopFileSystemFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( hadoopFileSystemFactory.create( namedCluster ) ).thenThrow( new IOException() );
    assertNull( hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) );
  }

  @Test
  public void testNormal() throws IOException, ClusterInitializationException {
    when( hadoopFileSystemFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( hadoopFileSystemFactory.create( namedCluster, null ) ).thenReturn( hadoopFileSystem );
    assertEquals( hadoopFileSystem, hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) );
  }

  @Test
  public void testNoEligibleFactories() throws IOException, ClusterInitializationException {
    when( hadoopFileSystemFactory.canHandle( namedCluster ) ).thenReturn( false );
    assertNull( hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) );
    verify( hadoopFileSystemFactory ).canHandle( namedCluster );
    verifyNoMoreInteractions( hadoopFileSystemFactory );
  }
}
