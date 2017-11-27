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
package org.pentaho.hadoop.shim.common.format.orc;

import org.apache.orc.TypeDescription;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.util.Iterator;

/**
 * Converts a SchemaDescription to a Orc Structure
 * Created by tkafalas on 11/3/2017.
 */
public class OrcSchemaConverter {
  private SchemaDescription schemaDescription;

  public TypeDescription buildTypeDescription( SchemaDescription schemaDescription ) {
    TypeDescription typeDescription = TypeDescription.createStruct();
    schemaDescription.forEach( f -> addStructField( typeDescription, f ) );
    return typeDescription;
  }

  private void addStructField( TypeDescription typeDescription, SchemaDescription.Field f ) {
    typeDescription.addField( f.formatFieldName, determineOrcType( f ) );
  }

  private TypeDescription determineOrcType( SchemaDescription.Field f ) {
    switch ( f.pentahoValueMetaType ) {
      case ValueMetaInterface.TYPE_NUMBER:
        return TypeDescription.createDouble();
      case ValueMetaInterface.TYPE_INET:
      case ValueMetaInterface.TYPE_STRING:
        return TypeDescription.createString();
      case ValueMetaInterface.TYPE_BOOLEAN:
        return TypeDescription.createBoolean();
      case ValueMetaInterface.TYPE_INTEGER:
        return TypeDescription.createLong();
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return TypeDescription.createDecimal().withPrecision( 20 ).withScale( 10 );
      case ValueMetaInterface.TYPE_SERIALIZABLE:
        return TypeDescription.createBinary();
      case ValueMetaInterface.TYPE_BINARY:
        return TypeDescription.createBinary();
      case ValueMetaInterface.TYPE_DATE:
        return TypeDescription.createDate();
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return TypeDescription.createTimestamp();
      default:
        throw new RuntimeException( "Field: " + f.formatFieldName + "  Undefined type: " + f.pentahoValueMetaType );
    }
  }

  /**
   * Convert the Orc TypeDescription Object to our SchemaDescription
   *
   * @param typeDescription
   * @return schemaDescription
   */
  public SchemaDescription buildSchemaDescription( TypeDescription typeDescription ) {
    SchemaDescription schemaDesc = new SchemaDescription();
    Iterator fieldNameIterator = typeDescription.getFieldNames().iterator();
    for ( TypeDescription subDescription : typeDescription.getChildren() ) {
      //Assume getFieldNames is 1:1 with getChildren
      String fieldName = (String) fieldNameIterator.next();
      int metaType = determineMetaType( subDescription );
      if ( metaType == -1 ) {
        throw new IllegalStateException(
          "Orc Field Name: " + fieldName + " - Could not find pdi field type for " + subDescription.getCategory()
            .getName() );
      }
      schemaDesc.addField( schemaDesc.new Field( fieldName, fieldName + "",
        determineMetaType( subDescription ), true ) );
    }
    return schemaDesc;
  }

  private int determineMetaType( TypeDescription subDescription ) {
    switch ( subDescription.getCategory().getName() ) {
      case "string":
        return ValueMetaInterface.TYPE_STRING;
      //TODO: Handle inet
      case "bigint":
        return ValueMetaInterface.TYPE_INTEGER;
      case "double":
        return ValueMetaInterface.TYPE_NUMBER;
      case "decimal":
        return ValueMetaInterface.TYPE_BIGNUMBER;
      case "timestamp":
        return ValueMetaInterface.TYPE_TIMESTAMP;
      case "date":
        return ValueMetaInterface.TYPE_DATE;
      case "boolean":
        return ValueMetaInterface.TYPE_BOOLEAN;
      case "binary":
        return ValueMetaInterface.TYPE_BINARY;
    }
    //if none of the cases match return a -1
    return -1;
  }

}
