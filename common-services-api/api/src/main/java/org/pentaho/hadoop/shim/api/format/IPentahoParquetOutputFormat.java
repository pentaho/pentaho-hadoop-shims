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

public interface IPentahoParquetOutputFormat extends IPentahoOutputFormat {
  enum VERSION {
    VERSION_1_0, VERSION_2_0
  }

  enum COMPRESSION {
    UNCOMPRESSED, SNAPPY, GZIP, LZO
  }

  void setSchema( SchemaDescription schema ) throws Exception;

  void setOutputFile( String file, boolean override ) throws Exception;

  void setVersion( VERSION ver ) throws Exception;

  void enableDictionary( boolean useDictionary ) throws Exception;

  void setCompression( COMPRESSION comp ) throws Exception;

  /**
   * Sets row group size
   *
   * @param size
   *          size in bytes
   */
  void setRowGroupSize( int size ) throws Exception;

  /**
   * Sets page size for compression
   *
   * @param size
   *          size in bytes
   */
  void setDataPageSize( int size ) throws Exception;

  /**
   *
   *
   * @param size
   *          size in bytes
   */
  void setDictionaryPageSize( int size ) throws Exception;
}
