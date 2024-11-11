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

public interface IPentahoOrcOutputFormat extends IPentahoOutputFormat, IPvfsAliasGenerator {
  int DEFAULT_COMPRESS_SIZE = 256; // In kilobytes
  int DEFAULT_STRIPE_SIZE = 64; // In megabytes
  int DEFAULT_ROW_INDEX_STRIDE = 10000; // In rows

  String STRIPE_SIZE_KEY = "orc.stripe.size";
  String COMPRESSION_KEY = "orc.compress";
  String COMPRESS_SIZE_KEY = "orc.compress.size";
  String ROW_INDEX_STRIDE_KEY = "orc.row.index.stride";
  String CREATE_INDEX_KEY = "orc.create.index";

  enum COMPRESSION {
    NONE, SNAPPY, ZLIB, LZO
  }

  void setFields( List<? extends IOrcOutputField> fields ) throws Exception;

  void setOutputFile( String file, boolean override ) throws Exception;

  void setCompression( COMPRESSION compression );

  void setStripeSize( int megabytes );

  void setRowIndexStride( int numRows );

  void setCompressSize( int kilobytes );

}
