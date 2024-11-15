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

package org.pentaho.hadoop.shim.api.format;

public interface IParquetInputField extends IFormatInputField {
  void setParquetType( ParquetSpec.DataType parquetType );

  void setParquetType( String parquetType );

  ParquetSpec.DataType getParquetType();

  static String marshall( IParquetInputField field ) {
    StringBuilder sb = new StringBuilder( 256 );
    sb.append( c( field.getFormatFieldName() ) );
    sb.append( "|" );
    sb.append( c( field.getPentahoFieldName() ) );
    sb.append( "|" );
    sb.append( Integer.toString( field.getFormatType() ) );
    sb.append( "|" );
    sb.append( Integer.toString( field.getPentahoType() ) );
    sb.append( "|" );
    sb.append( Integer.toString( field.getPrecision() ) );
    sb.append( "|" );
    sb.append( Integer.toString( field.getScale() ) );
    sb.append( "|" );
    sb.append( field.getStringFormat() );
    return sb.toString();
  }

  static String c( String s ) {
    if ( s == null ) {
      return "";
    }
    if ( s.contains( "|" ) ) {
      throw new RuntimeException( "Wrong value: " + s );
    }
    return s;
  }

  static String uc( String s ) {
    if ( s != null && s.isEmpty() ) {
      return null;
    }
    return s;
  }
}
