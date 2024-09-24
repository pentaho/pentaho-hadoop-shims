/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.hdi.format.orc;

import org.pentaho.hadoop.shim.HadoopShim;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcInputFormat;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HDIOrcInputFormat extends PentahoOrcInputFormat {
  private final org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf;
  private final HadoopShim shim;

  public HDIOrcInputFormat( NamedCluster namedCluster ) {
    super( namedCluster );
    shim = new HadoopShim();
    pentahoConf = shim.createConfiguration( namedCluster );
  }

  @Override
  public IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) {
    requireNonNull( fileName, NOT_NULL_MSG );
    requireNonNull( inputFields, NOT_NULL_MSG );
    return inClassloader( () -> new HDIOrcRecordReader( fileName, conf, inputFields, shim, pentahoConf ) );
  }

  @Override
  public List<IOrcInputField> readSchema() {
    return inClassloader( () -> readSchema(
            HDIOrcRecordReader.getReader(
                    requireNonNull( fileName, NOT_NULL_MSG ), conf, shim, pentahoConf ) ) );
  }
}