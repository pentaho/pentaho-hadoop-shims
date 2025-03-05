/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.format.parquet;


import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;

import java.util.ArrayList;
import java.util.List;

public class ParquetUtils {
  public static List<IParquetInputField> createSchema( int ageType ) {
    List<IParquetInputField> fields = new ArrayList<>();
    fields.add( new ParquetInputField( "Name", ParquetSpec.DataType.UTF8, "Name", ValueMetaInterface.TYPE_STRING ) );
    fields.add( new ParquetInputField( "Age", ParquetSpec.DataType.INT_64, "Age", ageType ) );
    return fields;
  }

  public static List<ParquetOutputField> createOutputFields() {
    return createOutputFields( ParquetSpec.DataType.UTF8 );
  }


  public static List<ParquetOutputField> createOutputFields( ParquetSpec.DataType ageType ) {
    return createOutputFields( ParquetSpec.DataType.UTF8, true, ageType, true );
  }

  public static List<ParquetOutputField> createOutputFields( ParquetSpec.DataType nameType, boolean nameAllowNull,
                                                             ParquetSpec.DataType ageType, boolean ageAllowNull ) {
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
    List<IParquetInputField> fields = new ArrayList<>();
    fields.add( new ParquetInputField( "Name", ParquetSpec.DataType.UTF8, "Name", nameType ) );
    fields.add( new ParquetInputField( "Age", ParquetSpec.DataType.INT_64, "Age", ageType ) );
    return fields;
  }
}
