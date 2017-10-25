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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import org.apache.hadoop.conf.Configuration;
//$import parquet.hadoop.api.InitContext;
//$import parquet.hadoop.api.ReadSupport;
//$import parquet.io.api.RecordMaterializer;
//$import parquet.schema.MessageType;
//$import parquet.schema.Type;
//#endif

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.parquet.ParquetConverter.MyRecordMaterializer;

public class PentahoParquetReadSupport extends ReadSupport<RowMetaAndData> {
  ParquetConverter converter;
  SchemaDescription schema;

  @Override
  public ReadContext init( InitContext context ) {
    String schemaStr = context.getConfiguration().get( ParquetConverter.PARQUET_SCHEMA_CONF_KEY );
    if ( schemaStr == null ) {
      throw new RuntimeException( "Schema not defined in the PentahoParquetSchema key" );
    }
    schema = SchemaDescription.unmarshall( schemaStr );

    converter = new ParquetConverter( schema );

    // get all fields from file's schema
    MessageType fileSchema = context.getFileSchema();
    Map<String, Type> fileFields = new TreeMap<>();
    for ( Type f : fileSchema.getFields() ) {
      fileFields.put( f.getName(), f );
    }
    fileSchema.getFields().clear();

    // use only required fields
    for ( SchemaDescription.Field f : schema ) {
      Type ff = fileFields.get( f.formatFieldName );
      if ( ff == null ) {
        throw new RuntimeException( "Field " + f.formatFieldName + " not found in parquet file" );
      } else {
        fileSchema.getFields().add( ff );
      }
    }

    return new ReadContext( fileSchema, new HashMap<>() );
  }

  @Override
  public RecordMaterializer<RowMetaAndData> prepareForRead( Configuration configuration,
                                                            Map<String, String> keyValueMetaData,
                                                            MessageType fileSchema, ReadContext readContext ) {
    return new MyRecordMaterializer( converter );
  }
}
