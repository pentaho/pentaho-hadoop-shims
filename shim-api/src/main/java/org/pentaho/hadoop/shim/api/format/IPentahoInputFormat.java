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

import java.io.Closeable;
import java.util.List;

import org.pentaho.di.core.RowMetaAndData;

public interface IPentahoInputFormat {

  /**
   * Get split parts.
   */
  List<IPentahoInputSplit> getSplits() throws Exception;

  /**
   * Read one split part.
   */
  IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) throws Exception;

  public interface IPentahoInputSplit {
  }

  public interface IPentahoRecordReader extends Iterable<RowMetaAndData>, Closeable {
  }
}
