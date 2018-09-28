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
package org.pentaho.hadoop.shim.common.format.avro;

import org.apache.avro.Schema;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;

import java.util.ArrayList;
import java.util.List;

public class AvroNestedFieldGetter {
  /**
   * Builds a list of field objects holding paths corresponding to the leaf primitives in an Avro schema.
   *
   * @param s the schema to process
   * @return a List of field objects
   * @throws KettleException if a problem occurs
   */
  public static List<? extends IAvroInputField> getLeafFields( Schema s ) throws Exception {
    if ( s == null ) {
      return null;
    }

    List<AvroInputField> fields = new ArrayList<AvroInputField>();

    String root = "";

    if ( s.getType() == Schema.Type.ARRAY || s.getType() == Schema.Type.MAP ) {
      while ( s.getType() == Schema.Type.ARRAY || s.getType() == Schema.Type.MAP ) {
        if ( s.getType() == Schema.Type.ARRAY ) {
          root += "[0]";
          s = s.getElementType();
        } else {
          root += "[*key*]";
          s = s.getValueType();
        }
      }
    }

    if ( s.getType() == Schema.Type.RECORD ) {
      processRecord( root, s, fields );
    } else if ( s.getType() == Schema.Type.UNION ) {
      processUnion( root, s, fields );
    } else {

      // our top-level array/map structure bottoms out with primitive types
      // we'll create one zero-indexed path through to a primitive - the
      // user can copy and paste the path if they want to extract other
      // indexes out to separate Kettle fields
      AvroInputField newField = createAvroField( root, s );
      if ( newField != null ) {
        fields.add( newField );
      }
    }

    for (int i = 0; i < fields.size() - 1; i++) {
      AvroInputField field = fields.get( i );
      boolean duplicateName;
      int suffix = 0;
      String fieldName;
      do {
        fieldName = field.getPentahoFieldName();
        if ( suffix > 0 ) {
          fieldName = fieldName + "-" + Integer.toString( suffix );
        }
        duplicateName = false;
        for ( int j = i + 1; ( j < fields.size() ) && !duplicateName; j++ ) {
          duplicateName = fieldName.equals( fields.get( j ).getPentahoFieldName() );
        }
        suffix++;
      } while ( duplicateName );

      field.setPentahoFieldName( fieldName );
    }
    return fields;
  }

  /**
   * Helper function used to build paths automatically when extracting leaf fields from a schema
   *
   * @param path   the path so far
   * @param s      the schema
   * @param fields a list of field objects that will correspond to leaf primitives
   * @throws KettleException if a problem occurs
   */
  protected static void processRecord( String path, Schema s, List<AvroInputField> fields ) throws KettleException {

    if ( path.length() > 0 ) {
      path += ".";
    }

    List<Schema.Field> recordFields = s.getFields();
    for ( Schema.Field rField : recordFields ) {
      Schema rSchema = rField.schema();
      /*
       * if (rSchema.getType() == Schema.Type.UNION) { rSchema = checkUnion(rSchema); }
       */

      if ( rSchema.getType() == Schema.Type.UNION ) {
        processUnion( path + rField.name(), rSchema, fields );
      } else if ( rSchema.getType() == Schema.Type.RECORD ) {
        processRecord( path + rField.name(), rSchema, fields );
      } else if ( rSchema.getType() == Schema.Type.ARRAY ) {
        processArray( path + rField.name() + "[0]", rSchema, fields );
      } else if ( rSchema.getType() == Schema.Type.MAP ) {
        processMap( path + rField.name() + "[*key*]", rSchema, fields );
      } else {
        // primitive
        AvroInputField newField =
          createAvroField( path + rField.name(), rSchema );
        if ( newField != null ) {
          fields.add( newField );
        }
      }
    }
  }

  /**
   * Helper function used to build paths automatically when extracting leaf fields from a schema
   *
   * @param path   the path so far
   * @param s      the schema
   * @param fields a list of field objects that will correspond to leaf primitives
   * @throws KettleException if a problem occurs
   */
  protected static void processUnion( String path, Schema s, List<AvroInputField> fields )
    throws KettleException {

    // first check for the presence of primitive/leaf types in this union
    List<Schema> primitives = checkUnionForLeafTypes( s );
    if ( primitives.size() > 0 ) {
      // if there is exactly one primitive then we can set the kettle type
      // for this primitive's type. If there is more than one primitive
      // then we'll have to use String to cover them all
      if ( primitives.size() == 1 ) {
        Schema single = primitives.get( 0 );
        AvroInputField newField = createAvroField( path, single );
        if ( newField != null ) {
          fields.add( newField );
        }
      } else {
        Schema stringS = Schema.create( Schema.Type.STRING );
        AvroInputField newField =
          createAvroField( path, stringS );
        if ( newField != null ) {
          fields.add( newField );
        }
      }
    }

    // now scan for arrays, maps and records. Unions may not immediately contain
    // other unions (according to the spec)
    for ( Schema toCheck : s.getTypes() ) {
      if ( toCheck.getType() == Schema.Type.RECORD ) {
        String recordName = "[u:" + toCheck.getName() + "]";

        processRecord( path, toCheck, fields );
      } else if ( toCheck.getType() == Schema.Type.MAP ) {
        processMap( path + "[*key*]", toCheck, fields );
      } else if ( toCheck.getType() == Schema.Type.ARRAY ) {
        processArray( path + "[0]", toCheck, fields );
      }
    }
  }

  /**
   * Helper function used to build paths automatically when extracting leaf fields from a schema
   *
   * @param path   the path so far
   * @param s      the schema
   * @param fields a list of field objects that will correspond to leaf primitives
   * @throws KettleException if a problem occurs
   */
  protected static void processMap( String path, Schema s, List<AvroInputField> fields )
    throws KettleException {

    s = s.getValueType(); // type of the values of the map

    if ( s.getType() == Schema.Type.UNION ) {
      processUnion( path, s, fields );
    } else if ( s.getType() == Schema.Type.ARRAY ) {
      processArray( path + "[0]", s, fields );
    } else if ( s.getType() == Schema.Type.RECORD ) {
      processRecord( path, s, fields );
    } else if ( s.getType() == Schema.Type.MAP ) {
      processMap( path + "[*key*]", s, fields );
    } else {
      AvroInputField newField = createAvroField( path, s );
      if ( newField != null ) {
        fields.add( newField );
      }
    }
  }

  /**
   * Helper function used to build paths automatically when extracting leaf fields from a schema
   *
   * @param path   the path so far
   * @param s      the schema
   * @param fields a list of field objects that will correspond to leaf primitives
   * @throws KettleException if a problem occurs
   */
  protected static void processArray( String path, Schema s, List<AvroInputField> fields )
    throws KettleException {

    s = s.getElementType(); // type of the array elements

    if ( s.getType() == Schema.Type.UNION ) {
      processUnion( path, s, fields );
    } else if ( s.getType() == Schema.Type.ARRAY ) {
      processArray( path + "[0]", s, fields );
    } else if ( s.getType() == Schema.Type.RECORD ) {
      processRecord( path, s, fields );
    } else if ( s.getType() == Schema.Type.MAP ) {
      processMap( path + "[*key*]", s, fields );
    } else {
      AvroInputField newField = createAvroField( path, s );
      if ( newField != null ) {
        fields.add( newField );
      }
    }
  }

  /**
   * Helper function that creates a field object once we've reached a leaf in the schema.
   *
   * @param path the path so far
   * @param s    the schema for the primitive
   * @return an avro field object.
   */
  protected static AvroInputField createAvroField( String path, Schema s ) {
    AvroInputField newField = new AvroInputField();

    String fieldName = "data";
    if ( path.trim().length() > 0 ) {
      fieldName = path.substring( path.lastIndexOf( "." ) + 1 );
    }
    int index = fieldName.indexOf( "[" );
    if ( index == 0 ) {
      fieldName = "data";
    } else if ( index > 0 ) {
      fieldName = fieldName.substring( 0, index );
    }
    newField.setAvroFieldName( path ); // set the name to the path, so that for
    newField.setPentahoFieldName( fieldName );
    switch ( s.getType() ) {
      case BOOLEAN:
        newField.setAvroType( AvroSpec.DataType.BOOLEAN );
        newField.setPentahoType( ValueMetaInterface.TYPE_BOOLEAN );
        break;
      case ENUM:
      case STRING:
        newField.setPentahoType( ValueMetaInterface.TYPE_STRING );
        newField.setAvroType( AvroSpec.DataType.STRING );
        if ( s.getType() == Schema.Type.ENUM ) {
          //TODO: Fix this - newField.m_indexedVals = s.getEnumSymbols();
        }
        break;
      case FLOAT:
        newField.setAvroType( AvroSpec.DataType.FLOAT );
        newField.setPentahoType( ValueMetaInterface.TYPE_NUMBER );
        break;
      case DOUBLE:
        newField.setAvroType( AvroSpec.DataType.DOUBLE );
        newField.setPentahoType( ValueMetaInterface.TYPE_NUMBER );
        break;
      case INT:
        if ( s.getLogicalType() != null ) {
          if ( s.getLogicalType().getName().equalsIgnoreCase( "Date" ) ) {
            newField.setAvroType( AvroSpec.DataType.DATE );
            newField.setPentahoType( ValueMetaInterface.TYPE_DATE );
          } else {
            newField.setAvroType( AvroSpec.DataType.INTEGER );
            newField.setPentahoType( ValueMetaInterface.TYPE_INTEGER );
          }
        } else {
          newField.setAvroType( AvroSpec.DataType.INTEGER );
          newField.setPentahoType( ValueMetaInterface.TYPE_INTEGER );
        }
        break;
      case LONG:
        newField.setAvroType( AvroSpec.DataType.LONG );
        newField.setPentahoType( ValueMetaInterface.TYPE_INTEGER );
        break;
      case BYTES:
        if ( s.getLogicalType() != null ) {
          if ( s.getLogicalType().getName().equalsIgnoreCase( "Decimal" ) ) {
            newField.setAvroType( AvroSpec.DataType.DECIMAL );
            newField.setPentahoType( ValueMetaInterface.TYPE_BIGNUMBER );
          } else {
            newField.setAvroType( AvroSpec.DataType.BYTES );
            newField.setPentahoType( ValueMetaInterface.TYPE_BINARY );
          }
        } else {
          newField.setAvroType( AvroSpec.DataType.BYTES );
          newField.setPentahoType( ValueMetaInterface.TYPE_BINARY );
        }
        break;
      case FIXED:
        newField.setAvroType( AvroSpec.DataType.FIXED );
        newField.setPentahoType( ValueMetaInterface.TYPE_BINARY );
        break;
      default:
        // unhandled type
        newField = null;
    }

    return newField;
  }

  /**
   * Check the supplied union for primitive/leaf types
   *
   * @param s the union schema to check
   * @return a list of primitive/leaf types in this union
   */
  protected static List<Schema> checkUnionForLeafTypes( Schema s ) {

    List<Schema> types = s.getTypes();
    List<Schema> primitives = new ArrayList<Schema>();

    for ( Schema toCheck : types ) {
      switch ( toCheck.getType() ) {
        case BOOLEAN:
        case LONG:
        case DOUBLE:
        case BYTES:
        case ENUM:
        case STRING:
        case INT:
        case FLOAT:
        case FIXED:
          primitives.add( toCheck );
          break;
      }
    }

    return primitives;
  }
}
