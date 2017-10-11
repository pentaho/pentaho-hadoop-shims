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
package org.pentaho.hbase.factory;

import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;

public abstract class HBaseClientFactoryLocator {

  private static ServiceLoader<HBaseClientFactoryLocator> sl = ServiceLoader.load( HBaseClientFactoryLocator.class );

  public static HBaseClientFactory getHBaseClientFactory( Configuration conf ) {
    for ( HBaseClientFactoryLocator hbcfl: sl ) {
      return hbcfl.createHBaseClientFactory( conf );
    }

    return null;
  }

  protected abstract HBaseClientFactory createHBaseClientFactory( Configuration conf );
}
