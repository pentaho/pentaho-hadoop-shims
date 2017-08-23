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
package org.pentaho.hadoop.shim.common.format;

import org.apache.hadoop.mapreduce.InputSplit;
import org.pentaho.hadoop.shim.api.format.PentahoInputSplit;

/**
 * Created by Vasilina_Terehova on 8/1/2017.
 */
public class PentahoInputSplitImpl implements PentahoInputSplit {
  InputSplit inputSplit;

  public PentahoInputSplitImpl( InputSplit inputSplit ) {
    this.inputSplit = inputSplit;
  }

  InputSplit getInputSplit() {
    return inputSplit;
  }
}
