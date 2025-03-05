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


package org.pentaho.hadoop.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.pentaho.di.core.exception.KettleException;

import java.util.Iterator;

/**
 * Executes a transformation as configured by the job's {@code Configuration} to reduce network traffic and disk I/O.
 */
public class GenericTransCombiner<K extends WritableComparable<?>, V extends Iterator<Writable>, K2, V2>
  extends GenericTransReduce<K, V, K2, V2> {

  public GenericTransCombiner() throws KettleException {
    this.setMRType( MROperations.Combine );
  }

  @Override
  public boolean isSingleThreaded() {
    return combineSingleThreaded;
  }

  @Override
  public String getInputStepName() {
    return combinerInputStepName;
  }

  @Override
  public String getOutputStepName() {
    return combinerOutputStepName;
  }
}
