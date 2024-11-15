/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.hadoop.shim.api.format;

import java.util.List;

public interface IPentahoParquetOutputFormat extends IPentahoOutputFormat, IPvfsAliasGenerator {
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
}
