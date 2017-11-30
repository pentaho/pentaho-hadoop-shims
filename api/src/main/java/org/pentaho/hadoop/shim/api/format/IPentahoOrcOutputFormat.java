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

public interface IPentahoOrcOutputFormat extends IPentahoOutputFormat {

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

  void setSchemaDescription( SchemaDescription schema ) throws Exception;

  void setOutputFile( String file ) throws Exception;

  void setCompression( COMPRESSION compression );

  void setStripeSize( int megabytes );

  void setRowIndexStride( int numRows );

  void setCompressSize( int kilobytes );

}
