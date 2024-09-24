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
package org.pentaho.hadoop.shim.api.format;

import java.util.List;

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
     * Extracts data from the output fields list that cannot be transferred to the native orc file and writes that data
     * as custom metaData to the orc file.
     *
     * @param fields
     */
    void write( List<? extends IOrcOutputField> fields );
  }

  interface Reader {
    /**
     * If the orc file has been written by PDI, there will be additional metadata stored in the custom metaData area of
     * the orc file.  This method extracts that data, if present, and adds it to a field list that was built soley by
     * the orc file TypeDescription.
     *
     * @param orcInputFields Presumeably a list of OrcInputFields built from the typeDescription alone (eg. <code>
     *                       OrcSchemaConverter.buildInputFields( TypeDescription ) </code>
     */
    void read( List<? extends IOrcInputField> orcInputFields );
  }
}
