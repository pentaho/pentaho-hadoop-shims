/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.hadoop.shim.common.format;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI" || shim_type=="MAPR"
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
//#endif
//#if shim_type=="CDH"
//$import parquet.hadoop.api.InitContext;
//$import parquet.hadoop.api.ReadSupport;
//$import parquet.io.api.RecordMaterializer;
//$import parquet.schema.MessageType;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.ParquetConverter.MyRecordMaterializer;

public class PentahoParquetReadSupport extends ReadSupport<RowMetaAndData> {
  ParquetConverter converter;

  @Override
  public ReadContext init( InitContext context ) {
    String schemaStr = context.getConfiguration().get( "PentahoParquetSchema" );
    if ( schemaStr == null ) {
      throw new RuntimeException( "Schema not defined in the PentahoParquetSchema key" );
    }
    converter = new ParquetConverter( SchemaDescription.unmarshall( schemaStr ) );

    System.out.println( context.getFileSchema() );

    return new ReadContext( converter.createParquetSchema(), new HashMap<String, String>() );
  }

  @Override
  public RecordMaterializer<RowMetaAndData> prepareForRead( Configuration configuration,
      Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext ) {
    return new MyRecordMaterializer( converter );
  }
}
