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
package org.pentaho.big.data.impl.shim.mapreduce;

import org.pentaho.bigdata.api.mapreduce.MapReduceTransformations;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;

/**
 * Created by ccaspanello on 8/29/2016.
 */
public class MockVisitorService implements TransformationVisitorService {
  @Override
  public void visit( MapReduceTransformations transformations, NamedCluster namedCluster ) {
    // Do Nothing
  }
}
