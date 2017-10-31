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
package org.pentaho.hadoop.shim.api.format;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Schema description for all Hadoop formats(Parquet/Avro/ORC).
 * 
 * @author Alexander Buloichik
 */
public class SchemaDescription implements Iterable<SchemaDescription.Field> {

  private final List<Field> fields = new ArrayList<>();

  public void addField( Field field ) {
    fields.add( field );
  }

  public String marshall() {
    StringBuilder o = new StringBuilder( 2048 );
    for ( Field f : fields ) {
      o.append( f.marshall() ).append( '\n' );
    }
    return o.toString();
  }

  public static SchemaDescription unmarshall( String str ) {
    SchemaDescription r = new SchemaDescription();
    if ( str.isEmpty() ) {
      return r;
    }
    String[] lines = str.split( "\n" );
    for ( String line : lines ) {
      r.addField( r.unmarshallField( line ) );
    }
    return r;
  }

  public Field unmarshallField( String str ) {
    String[] values = new String[5];
    int prev = 0;
    for ( int i = 0; i < 4; i++ ) {
      int pos = str.indexOf( '|', prev );
      if ( pos < 0 ) {
        throw new RuntimeException( "Wrong field: " + str );
      }
      values[i] = str.substring( prev, pos );
      prev = pos + 1;
    }
    if ( str.indexOf( '|', prev ) >= 0 ) {
      throw new RuntimeException( "Wrong field: " + str );
    }
    values[4] = str.substring( prev );

    Field r =
        new Field( uc( values[0] ), uc( values[1] ), Integer.parseInt( values[2] ), Boolean.parseBoolean( values[4] ) );
    r.defaultValue = uc( values[3] );
    return r;
  }

  public boolean isEmpty() {
    return fields.isEmpty();
  }

  @Override
  public Iterator<Field> iterator() {
    return fields.iterator();
  }

  @Override
  public String toString() {
    return marshall();
  }

  public class Field {
    /**
     * Field name in the Hadoop format file.
     */
    public String formatFieldName;

    /**
     * Field file in the Kettle row.
     */
    public String pentahoFieldName;

    /**
     * Type id from ValueMetaInterface.
     */
    public int pentahoValueMetaType;

    public String defaultValue;
    public boolean allowNull;

    public Field( String formatFieldName, String pentahoFieldName, int pentahoValueMetaType, boolean allowNull ) {
      this.formatFieldName = formatFieldName;
      this.pentahoFieldName = pentahoFieldName;
      this.pentahoValueMetaType = pentahoValueMetaType;
      this.allowNull = allowNull;
    }

    public Field( String formatFieldName, String pentahoFieldName, int pentahoValueMetaType, String defaultValue, boolean allowNull ) {
      this.formatFieldName = formatFieldName;
      this.pentahoFieldName = pentahoFieldName;
      this.pentahoValueMetaType = pentahoValueMetaType;
      this.defaultValue = defaultValue;
      this.allowNull = allowNull;
    }

    public String marshall() {
      StringBuilder o = new StringBuilder( 256 );
      o.append( c( formatFieldName ) );
      o.append( '|' );
      o.append( c( pentahoFieldName ) );
      o.append( '|' );
      o.append( Integer.toString( pentahoValueMetaType ) );
      o.append( '|' );
      o.append( c( defaultValue ) );
      o.append( '|' );
      o.append( Boolean.toString( allowNull ) );
      return o.toString();
    }
  }

  public static String c( String s ) {
    if ( s == null ) {
      return "";
    }
    if ( s.contains( "|" ) ) {
      throw new RuntimeException( "Wrong value: " + s );
    }
    return s;
  }

  public static String uc( String s ) {
    if ( s != null && s.isEmpty() ) {
      return null;
    }
    return s;
  }

  public Field getField( String pentahoFieldName ) {
    if ( pentahoFieldName == null || pentahoFieldName.trim().isEmpty() ) {
      return null;
    }

    for ( Field field : fields ) {
      if ( field.pentahoFieldName.equals( pentahoFieldName ) ) {
        return field;
      }
    }

    return null;
  }
  public Field getFormatField( String formatFieldName ) {
    if ( formatFieldName == null || formatFieldName.trim().isEmpty() ) {
      return null;
    }

    for ( Field field : fields ) {
      if ( field.formatFieldName.equals( formatFieldName ) ) {
        return field;
      }
    }

    return null;
  }
}
