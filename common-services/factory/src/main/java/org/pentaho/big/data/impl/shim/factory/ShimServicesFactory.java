/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.factory;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseServiceImpl;
import com.pentaho.big.data.bundles.impl.shim.hdfs.HadoopFileSystemImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.osgi.framework.BundleContext;
import org.pentaho.big.data.impl.shim.format.FormatServiceImpl;
import org.pentaho.big.data.impl.shim.mapreduce.MapReduceServiceImpl;
import org.pentaho.big.data.impl.shim.oozie.FallbackOozieClientImpl;
import org.pentaho.big.data.impl.shim.oozie.OozieServiceImpl;
import org.pentaho.big.data.impl.shim.pig.PigServiceImpl;
import org.pentaho.big.data.impl.shim.sqoop.SqoopServiceImpl;
import org.pentaho.big.data.impl.shim.mapreduce.TransformationVisitorService;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.api.internal.Configuration;
import org.pentaho.hadoop.shim.api.ShimServicesFactoryInterface;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceService;
import org.pentaho.hadoop.shim.api.oozie.OozieService;
import org.pentaho.hadoop.shim.api.pig.PigService;
import org.pentaho.hadoop.shim.api.sqoop.SqoopService;
import org.pentaho.hadoop.shim.common.CommonFormatShim;
import org.pentaho.hadoop.shim.common.pig.CommonPigShim;
import org.pentaho.hadoop.shim.common.pig.PigShimImpl;
import org.pentaho.hadoop.shim.common.sqoop.CommonSqoopShim;
import org.pentaho.hadoop.shim.spi.FormatShim;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.pentaho.hbase.shim.common.CommonHBaseShim;
import org.pentaho.hbase.shim.spi.HBaseShim;
import org.pentaho.oozie.shim.api.OozieClient;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class ShimServicesFactory implements ShimServicesFactoryInterface{
  private final FormatShim formatShim = new CommonFormatShim();
  private final SqoopShim sqoopShim = new CommonSqoopShim();
  private final HBaseShim hbaseShim = new CommonHBaseShim();
  private final PigShim pigShim = new PigShimImpl();
  private HadoopShim hadoopShim;
  private BundleContext bundleContext;
  private ExecutorService mapReduceExecutorService;
  private List<TransformationVisitorService> mapReduceVisitorServices;

  public ShimServicesFactory(HadoopShim hadoopShim, ExecutorService mapReduceExecutorService, List<TransformationVisitorService> mapReduceVisitorServices) {
    this.hadoopShim = hadoopShim;
    this.mapReduceExecutorService = mapReduceExecutorService;
    this.mapReduceVisitorServices = mapReduceVisitorServices;
  }

  @Override
  public FormatService createFormatService(NamedCluster namedCluster) {
    return new FormatServiceImpl(namedCluster, formatShim);
  }

  @Override
  public SqoopService createSqoopService(NamedCluster namedCluster) {
    return new SqoopServiceImpl( hadoopShim, sqoopShim, namedCluster );
  }

  @Override
  public PigService createPigService(NamedCluster namedCluster) {
    return new PigServiceImpl( namedCluster, pigShim, hadoopShim );
  }

  @Override
  public MapReduceService createMapReduceService(NamedCluster namedCluster) {
    return new MapReduceServiceImpl( namedCluster, hadoopShim, mapReduceExecutorService, mapReduceVisitorServices );
  }

  @Override
  public OozieService createOozieService( NamedCluster namedCluster ) {
    String oozieUrl = namedCluster.getOozieUrl();
    OozieClient client = new FallbackOozieClientImpl( new org.apache.oozie.client.OozieClient( oozieUrl ) );
    return new OozieServiceImpl( client, namedCluster );
  }

  @Override
  public HBaseService createHBaseService( NamedCluster namedCluster ) throws ConfigurationException {
    return new HBaseServiceImpl( namedCluster, hbaseShim );
  }

  @Override
  public HadoopFileSystem createHadoopFileSystem( NamedCluster namedCluster ) throws IOException {
    return createHadoopFileSystem( namedCluster, null );
  }

  @Override
  public HadoopFileSystem createHadoopFileSystem( NamedCluster namedCluster, URI uri ) throws IOException {
    final Configuration configuration = hadoopShim.createConfiguration( namedCluster.getConfigId() );
    FileSystem fileSystem = (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();
    if ( fileSystem instanceof LocalFileSystem ) {
      throw new IOException( "Got a local filesystem, was expecting an hdfs connection" );
    }

    final URI finalUri = fileSystem.getUri() != null ? fileSystem.getUri() : uri;
    HadoopFileSystem hadoopFileSystem = new HadoopFileSystemImpl( () -> {
      try {
        return finalUri != null ? (FileSystem) hadoopShim.getFileSystem( finalUri, configuration, (NamedCluster) namedCluster ).getDelegate()
          : (FileSystem) hadoopShim.getFileSystem( configuration ).getDelegate();
      } catch ( IOException | InterruptedException e ) {
        return null;
      }
    } );
    ( (HadoopFileSystemImpl) hadoopFileSystem ).setNamedCluster( namedCluster );

    return hadoopFileSystem;
  }

  public BundleContext getBundleContext() {
    return bundleContext;
  }

  public void setBundleContext( BundleContext bundleContext ) {
    this.bundleContext = bundleContext;
    if (sqoopShim instanceof CommonSqoopShim) {
      ((CommonSqoopShim)sqoopShim).setBundleContext( bundleContext );
    }
    if (pigShim instanceof CommonPigShim ) {
      ((CommonPigShim)pigShim).setBundleContext( bundleContext );
    }

  }
}
