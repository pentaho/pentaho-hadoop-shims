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


import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.InputStream;
import java.util.List;

public interface IPentahoAvroInputFormat extends IPentahoInputFormat {

  /**
   * Set schema for file reading.
   */
  List<? extends IAvroInputField> getFields(  ) throws Exception;

  /**
     * Set schema for file reading.
     */
  void setInputFields( List<? extends IAvroInputField> fields ) throws Exception;

  /**
   * Set input file.
   */
  void setInputFile( String file ) throws Exception;

  /**
   * Set input file.
   */
  void setInputSchemaFile( String schemaFile ) throws Exception;

  /**
   * Split size, bytes.
   */
  void setSplitSize( long blockSize ) throws Exception;

  String getInputStreamFieldName();

  void setInputStreamFieldName( String inputStreamFieldName );

  boolean isUseFieldAsInputStream();

  void setInputStream( InputStream inputStream );

  boolean isComplex();

  void setIsComplex( boolean isComplex );

  void setVariableSpace( VariableSpace variableSpace );

  void setIncomingFields( Object[] incomingFields );

  void setOutputRowMeta( RowMetaInterface outputRowMeta);
}
