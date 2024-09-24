/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter;

import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.schema.Type;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.common.format.parquet.ParquetInputFieldList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PentahoParquetReadSupport extends ReadSupport<RowMetaAndData> {
  ParquetConverter converter;
  List<? extends IParquetInputField> fields;

  @Override
  public ReadContext init( InitContext context ) {
    String schemaStr = context.getConfiguration().get( ParquetConverter.PARQUET_SCHEMA_CONF_KEY );
    if ( schemaStr == null ) {
      throw new RuntimeException( "Schema not defined in the PentahoParquetSchema key" );
    }

    ParquetInputFieldList schema = ParquetInputFieldList.unmarshall( schemaStr );
    converter = new ParquetConverter( schema.getFields() );

    // get all fields from file's schema
    MessageType fileSchema = context.getFileSchema();
    List<Type> newFields = new ArrayList<>();
    // use only required fields
    for ( IParquetInputField f : schema ) {
      Type origField = fileSchema.getFields().get( fileSchema.getFieldIndex( f.getFormatFieldName() ) );
      newFields.add( origField );
    }
    if ( newFields.isEmpty() ) {
      throw new RuntimeException( "Fields should be declared" );
    }
    MessageType newSchema = new MessageType( fileSchema.getName(), newFields );

    return new ReadContext( newSchema, new HashMap<>() );
  }

  @Override
  public RecordMaterializer<RowMetaAndData> prepareForRead( Configuration configuration,
                                                            Map<String, String> keyValueMetaData,
                                                            MessageType fileSchema, ReadContext readContext ) {
    return new ParquetConverter.MyRecordMaterializer( converter );
  }
}
