/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.parquet;

import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.hadoop.api.WriteSupport;
//$import parquet.io.api.RecordConsumer;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

public class PentahoParquetWriteSupport extends WriteSupport<RowMetaAndData> {
  ParquetConverter converter;
  RecordConsumer consumer;

  public PentahoParquetWriteSupport( SchemaDescription schema ) {
    converter = new ParquetConverter( schema );
  }

  @Override
  public WriteContext init( Configuration configuration ) {
    try {
      return new WriteContext( converter.createParquetSchema(), new TreeMap<>() );
    } catch ( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }

  @Override
  public void prepareForWrite( RecordConsumer recordConsumer ) {
    consumer = recordConsumer;
  }

  @Override
  public void write( RowMetaAndData record ) {
    converter.writeRow( record, consumer );
  }
}
