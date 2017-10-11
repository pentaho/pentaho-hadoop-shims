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
package org.pentaho.hadoop.hbase.factory;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.pentaho.hbase.factory.HBasePut;

public class HBase10Put implements HBasePut {
  Put put;

  HBase10Put( byte[] key ) {
    this.put = new Put( key );
  }
  
  @Override
  public void setWriteToWAL( boolean writeToWAL ) {
    put.setDurability( writeToWAL ? Durability.USE_DEFAULT : Durability.SKIP_WAL );
  }

  @Override
  public void addColumn( byte[] colFamily, byte[] colName, byte[] colValue ) {
    put.addColumn( colFamily, colName, colValue );
  }

  Put getPut() {
    return put;
  }

}
