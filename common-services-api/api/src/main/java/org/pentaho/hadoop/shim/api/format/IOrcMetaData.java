/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

/**
 * Created by tkafalas on 11/22/2017.
 */
public interface IOrcMetaData {
  static final String ORC_CUSTOM_METADATA_PREFIX = "pentaho";
  static final String ORC_CUSTOM_METADATA_PROPERTY_DELIMITER = ".";

  enum propertyType {
    TYPE,
    NULLABLE,
    DEFAULT
  }

  static String determinePropertyName( String fieldName, String property ) {
    StringBuilder s = new StringBuilder();
    s.append( ORC_CUSTOM_METADATA_PREFIX ).append( ORC_CUSTOM_METADATA_PROPERTY_DELIMITER ).append( fieldName )
      .append( ORC_CUSTOM_METADATA_PROPERTY_DELIMITER ).append( property );
    return s.toString();
  }

  interface Writer {
    /**
     * Extracts data from the SchemaDescription that cannot be transferred to the native orc file and writes
     * that data as custom metaData to the orc file.
     * @param schemaDescription
     */
    void write( SchemaDescription schemaDescription );
  }

  interface Reader {
    /**
     * If the orc file has been written by PDI, there will be additional metadata stored in the custom metaData area
     * of the orc file.  This method extracts that data, if present, and adds it to a schemaDescription that was built
     * soley by the orc file TypeDescription.
     *
     * @param schemaDescription Presumeably a schema description built from the typeDescription alone (eg. <code>
     *                          OrcSchemaConverter.buildSchemaDescription( TypeDescription ) </code>
     */
    void read( SchemaDescription schemaDescription );
  }


}
