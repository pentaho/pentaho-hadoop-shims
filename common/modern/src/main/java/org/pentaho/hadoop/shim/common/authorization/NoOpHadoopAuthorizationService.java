/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.common.authorization;

import org.pentaho.hadoop.shim.common.ClassPathModifyingSqoopShim;
import org.pentaho.hadoop.shim.common.CommonFormatShim;
import org.pentaho.hadoop.shim.common.CommonHadoopShim;
import org.pentaho.hadoop.shim.common.CommonPigShim;
import org.pentaho.hadoop.shim.common.CommonSnappyShim;
import org.pentaho.hadoop.shim.common.CommonSqoopShim;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;
import org.pentaho.hadoop.shim.common.PigShimImpl;
import org.pentaho.hadoop.shim.common.SnappyShimImpl;
import org.pentaho.hadoop.shim.spi.FormatShim;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PentahoHadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.pentaho.hadoop.shim.spi.SnappyShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.pentaho.hbase.shim.common.CommonHBaseShim;
import org.pentaho.hbase.shim.common.HBaseShimImpl;
import org.pentaho.hbase.shim.common.wrapper.HBaseShimInterface;
import org.pentaho.oozie.shim.api.OozieClientFactory;

import java.util.HashMap;
import java.util.Map;

public class NoOpHadoopAuthorizationService implements HadoopAuthorizationService {
  private final Map<Class<?>, PentahoHadoopShim> shimMap;

  public NoOpHadoopAuthorizationService() {
    shimMap = new HashMap<Class<?>, PentahoHadoopShim>();
    shimMap.put( HadoopShim.class, getHadoopShim() );
    shimMap.put( PigShim.class, getPigShim() );
    shimMap.put( SnappyShim.class, getSnappyShim() );
    shimMap.put( FormatShim.class, getFormatShim() );
    shimMap.put( SqoopShim.class, getSqoopShim() );
    shimMap.put( HBaseShimInterface.class, getHbaseShim() );
    shimMap.put( OozieClientFactory.class, getOozieFactory() );
  }

  protected PentahoHadoopShim getOozieFactory() {
    try {
      return (PentahoHadoopShim) Class.forName(
        "org.pentaho.di.job.entries.oozie.OozieClientFactoryImpl" ).newInstance();
    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to create oozie client factory", e );
    }
  }

  protected CommonHBaseShim getHbaseShim() {
    return new HBaseShimImpl();
  }

  protected CommonSqoopShim getSqoopShim() {
    return new ClassPathModifyingSqoopShim();
  }

  protected CommonSnappyShim getSnappyShim() {
    return new SnappyShimImpl();
  }

  protected CommonPigShim getPigShim() {
    return new PigShimImpl();
  }

  protected CommonHadoopShim getHadoopShim() {
    return new HadoopShimImpl();
  }

  @SuppressWarnings( "unchecked" )
  @Override
  public synchronized <T extends PentahoHadoopShim> T getShim( Class<T> clazz ) {
    return (T) shimMap.get( clazz );
  }

  public PentahoHadoopShim getFormatShim() {
    return new CommonFormatShim();
  }
}
