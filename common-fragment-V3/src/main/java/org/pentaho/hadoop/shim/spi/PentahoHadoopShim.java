/*******************************************************************************
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

package org.pentaho.hadoop.shim.spi;

import org.pentaho.hadoop.shim.ShimVersion;

/**
 * Represents a type of Hadoop shim. Shims provide an abstraction over a set of
 * APIs that depend upon a set of specific Hadoop libraries. Their implementations
 * must be abstracted so that they may be swapped out at runtime.
 *
 */
public interface PentahoHadoopShim {
  /**
   * @return the version of this shim
   */
  ShimVersion getVersion();
}
