/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2020 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.pvfs.conf.providers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class PentahoAzureSasTokenProvider implements SASTokenProvider {

  private String sasToken;

  @Override
  public void initialize( Configuration configuration, String s ) {
    sasToken = configuration.get( "fs.azure.sas.token" );
  }

  @Override
  public String getSASToken( String s, String s1, String s2, String s3 ) {
    //We are not generating a SAS token but using a configured SAS Token.
    return sasToken;
  }
}
