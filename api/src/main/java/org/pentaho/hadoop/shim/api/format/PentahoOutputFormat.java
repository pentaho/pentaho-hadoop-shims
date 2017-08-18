/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

public interface PentahoOutputFormat {
  enum VERSION {
    VERSION_1_0, VERSION_2_0
  }

  enum ENCODING {
    PLAIN, DICTIONARY, BIT_PACKED, RLE
  }

  void setSchema( SchemaDescription schema );

  void setOutputDir( String dir );

  void setVersion( VERSION ver );

  void setEncoding( ENCODING enc );

  void setRowGroupSize( long size );

  void setDataPageSize( long size );

  void setDictionaryPageSize( long size );

  PentahoRecordWriter createRecordWriter();
}
