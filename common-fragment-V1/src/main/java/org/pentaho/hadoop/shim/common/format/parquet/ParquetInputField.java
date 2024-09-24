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
package org.pentaho.hadoop.shim.common.format.parquet;

import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.hadoop.shim.api.format.ParquetSpec.DataType;
import org.pentaho.hadoop.shim.common.format.BaseFormatInputField;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;

public class ParquetInputField extends BaseFormatInputField implements IParquetInputField {
  public ParquetInputField() {
  }

  public ParquetInputField( String formatFieldName, DataType dataType, String pentahoFieldName, int pentahoType ) {
    setFormatFieldName( formatFieldName );
    setParquetType( dataType );
    setPentahoFieldName( pentahoFieldName );
    setPentahoType( pentahoType );
  }

  @Override
  public DataType getParquetType() {
    return DataType.getDataType( getFormatType() );
  }

  @Override
  public void setParquetType( DataType parquetType ) {
    setFormatType( parquetType.getId() );
  }

  @Override
  public void setParquetType( String parquetType ) {
    for ( DataType tmpType : DataType.values() ) {
      if ( tmpType.getName().equalsIgnoreCase( parquetType ) ) {
        setFormatType( tmpType.getId() );
        break;
      }
    }
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( getPentahoType() );
  }

  private static String uc( String s ) {
    if ( s != null && s.isEmpty() ) {
      return null;
    }
    return s;
  }

  public static IParquetInputField unmarshallField( String str ) {
    String[] values = new String[ 7 ];
    int prev = 0;
    for ( int i = 0; i < 6; i++ ) {
      int pos = str.indexOf( '|', prev );
      if ( pos < 0 ) {
        throw new RuntimeException( "Wrong field: " + str );
      }
      values[ i ] = str.substring( prev, pos );
      prev = pos + 1;
    }
    if ( str.indexOf( '|', prev ) >= 0 ) {
      throw new RuntimeException( "Wrong field: " + str );
    }
    values[ 6 ] = str.substring( prev );

    ParquetInputField field = new ParquetInputField();
    field.setFormatFieldName( uc( values[ 0 ] ) );
    field.setPentahoFieldName( uc( values[ 1 ] ) );
    field.setFormatType( Integer.parseInt( values[ 2 ] ) );
    field.setPentahoType( Integer.parseInt( values[ 3 ] ) );
    field.setPrecision( Integer.parseInt( values[ 4 ] ) );
    field.setScale( Integer.parseInt( values[ 5 ] ) );
    field.setStringFormat( values[ 6 ] );
    return field;
  }
}
