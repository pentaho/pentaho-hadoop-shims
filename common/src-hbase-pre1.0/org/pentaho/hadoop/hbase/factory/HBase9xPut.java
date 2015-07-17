/*******************************************************************************
*
* Pentaho Big Data
*
* Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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
package org.pentaho.hadoop.hbase.factory;

import org.apache.hadoop.hbase.client.Put;
import org.pentaho.hbase.factory.HBasePut;

public class HBase9xPut implements HBasePut {
  org.apache.hadoop.hbase.client.Put put;
  
  public HBase9xPut( byte[] key ) {
    put = new Put( key );
  }

  @Override
  public void setWriteToWAL( boolean writeToWAL ) {
    put.setWriteToWAL( writeToWAL );
  }

  @Override
  public void addColumn( byte[] colFamily, byte[] colName, byte[] colValue ) {
    put.add( colFamily, colName, colValue );
  }
  
  Put getPut() {
    return this.put;
  }

}
