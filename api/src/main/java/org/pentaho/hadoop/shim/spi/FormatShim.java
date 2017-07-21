/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.spi;


import org.pentaho.hadoop.shim.api.format.FormatSchema;
import org.pentaho.hadoop.shim.api.format.InputFormat;
import org.pentaho.hadoop.shim.api.format.OutputFormat;
import org.pentaho.hadoop.shim.api.fs.Path;

public interface FormatShim extends PentahoHadoopShim {

  InputFormat inputFormat();

  InputFormat inputFormat( FormatSchema formatSchema );

  OutputFormat outputFormat();

  OutputFormat outputFormat( FormatSchema formatSchema );

  //TODO create configurable schema by calling this method?
  //FormatSchema formatSchema();

  FormatSchema formatSchema( Path path );

}
