/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
