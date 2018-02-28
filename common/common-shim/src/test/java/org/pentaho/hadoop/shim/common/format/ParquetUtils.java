/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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


import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;
import org.pentaho.hadoop.shim.common.format.parquet.ParquetInputField;
import org.pentaho.hadoop.shim.common.format.parquet.ParquetOutputField;

import java.util.ArrayList;
import java.util.List;

public class ParquetUtils {
  public static List<IParquetInputField> createSchema( int ageType ) {
    List<IParquetInputField> fields = new ArrayList<>(  );
    fields.add( new ParquetInputField( "Name", ParquetSpec.DataType.UTF8, "Name", ValueMetaInterface.TYPE_STRING ) );
    fields.add( new ParquetInputField( "Age", ParquetSpec.DataType.INT_64, "Age", ageType ) );
    return fields;
  }

  public static List<ParquetOutputField> createOutputFields(  ) {
    return createOutputFields( ParquetSpec.DataType.UTF8 );
  }


  public static List<ParquetOutputField> createOutputFields( ParquetSpec.DataType ageType ) {
    return createOutputFields( ParquetSpec.DataType.UTF8, true, ageType, true );
  }

  public static List<ParquetOutputField> createOutputFields( ParquetSpec.DataType nameType, boolean nameAllowNull, ParquetSpec.DataType ageType, boolean ageAllowNull ) {
    List<ParquetOutputField> fields = new ArrayList<>();

    ParquetOutputField outputField = new ParquetOutputField();
    outputField.setFormatFieldName( "Name" );
    outputField.setPentahoFieldName( "Name" );
    outputField.setFormatType( nameType );
    outputField.setAllowNull( nameAllowNull );
    fields.add( outputField );

    outputField = new ParquetOutputField();
    outputField.setFormatFieldName( "Age" );
    outputField.setPentahoFieldName( "Age" );
    outputField.setFormatType( ageType );
    outputField.setAllowNull( ageAllowNull );
    fields.add( outputField );

    return fields;
  }

  public static List<IParquetInputField> createSchema( int nameType, int ageType ) {
    List<IParquetInputField> fields = new ArrayList<>(  );
    fields.add( new ParquetInputField( "Name", ParquetSpec.DataType.UTF8, "Name", nameType ) );
    fields.add( new ParquetInputField( "Age", ParquetSpec.DataType.INT_64, "Age", ageType ) );
    return fields;
  }
}
