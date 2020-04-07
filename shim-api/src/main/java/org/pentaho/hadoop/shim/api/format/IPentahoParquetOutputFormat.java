/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2020 by Hitachi Vantara : http://www.pentaho.com
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

import java.util.List;

public interface IPentahoParquetOutputFormat extends IPentahoOutputFormat {
  enum VERSION {
    VERSION_1_0, VERSION_2_0
  }

  enum COMPRESSION {
    UNCOMPRESSED, SNAPPY, GZIP, LZO
  }

  void setFields( List<? extends IParquetOutputField> fields ) throws Exception;

  void setOutputFile( String file, boolean override ) throws Exception;

  void setVersion( VERSION ver ) throws Exception;

  void enableDictionary( boolean useDictionary ) throws Exception;

  void setCompression( COMPRESSION comp ) throws Exception;

  /**
   * Sets row group size
   *
   * @param size size in bytes
   */
  void setRowGroupSize( int size ) throws Exception;

  /**
   * Sets page size for compression
   *
   * @param size size in bytes
   */
  void setDataPageSize( int size ) throws Exception;

  /**
   * @param size size in bytes
   */
  void setDictionaryPageSize( int size ) throws Exception;

  /**
   * This method should be called on any url before using the  Hadoop FileSystem api.  If a non-null url is returned
   * then that url should be used to process a temporary staging file.  VFS should then be used to copy to this
   * temporary file to its final destination.
   * @param pvfsPath The uri of the file to be operated on.
   * @return The uri to use instead, or null if the path is ok as is.
   */
  public String generateAlias( String pvfsPath );
}
