/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.hadoop.shim.common.format.parquet;

import org.apache.hadoop.mapreduce.InputSplit;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoInputSplit;

/**
 * Created by Vasilina_Terehova on 8/1/2017.
 */
public class PentahoInputSplitImpl implements IPentahoInputSplit {
  InputSplit inputSplit;

  public PentahoInputSplitImpl( InputSplit inputSplit ) {
    this.inputSplit = inputSplit;
  }

  public InputSplit getInputSplit() {
    return inputSplit;
  }
}
