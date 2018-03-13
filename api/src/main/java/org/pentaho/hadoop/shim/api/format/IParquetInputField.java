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
package org.pentaho.hadoop.shim.api.format;

public interface IParquetInputField extends IFormatInputField {
  void setParquetType( ParquetSpec.DataType parquetType );

  void setParquetType( String parquetType );

  ParquetSpec.DataType getParquetType( );

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
