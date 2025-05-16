/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.hadoop.shim;

import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.internal.ShimIdentifier;
import org.pentaho.hadoop.shim.common.ConfigurationProxyV2;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;

public class HadoopShim extends HadoopShimImpl {

  private static final String ID = "apache";
  private static final String VENDOR = "Apache";
  private static final String VERSION = "3.4";
  private static final ShimIdentifierInterface.ShimType TYPE = ShimIdentifierInterface.ShimType.COMMUNITY;
  private static final ShimIdentifier SHIM_IDENTIFIER = new ShimIdentifier( ID, VENDOR, VERSION, TYPE );

  public HadoopShim() {
    super();
    S3NCredentialUtils.setS3nIsSupported( false );
  }

  @Override
  public Class[] getHbaseDependencyClasses() {
    return new Class[0];
  }

  @Override
  public org.pentaho.hadoop.shim.api.internal.Configuration createConfiguration( String namedClusterConfigId ) {
    ConfigurationProxyV2 conf = (ConfigurationProxyV2) super.createConfiguration( namedClusterConfigId );
    conf.getJob().getConfiguration().setRestrictSystemProperties( false );
    return conf;
  }

  @Override
  public ShimIdentifier getShimIdentifier() {
    return SHIM_IDENTIFIER;
  }

}
