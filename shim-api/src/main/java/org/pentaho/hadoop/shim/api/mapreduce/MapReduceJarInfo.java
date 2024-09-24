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

package org.pentaho.hadoop.shim.api.mapreduce;

import java.util.List;

/**
 * Created by bryan on 12/4/15.
 */
public interface MapReduceJarInfo {
  /**
   * Returns a list of classes in the jar that have a method with the signature: public static void main(String[] args)
   *
   * @return a list of classes in the jar that have a method with the signature: public static void main(String[] args)
   */
  List<String> getClassesWithMain();

  /**
   * Returns the main class as listed in the manifest of the jar
   *
   * @return the main class as listed in the manifest of the jar
   */
  String getMainClass();
}
