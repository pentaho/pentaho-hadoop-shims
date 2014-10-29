/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
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
package org.pentaho.hadoop.mapreduce;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import java.io.IOException;
import static org.junit.Assert.*;

/**
 * User: Dzmitry Stsiapanau Date: 10/29/14 Time: 12:44 PM
 */
public class GenericTransReduceTest {
  @Test
  public void testClose() throws KettleException , IOException {
    GenericTransReduce gtr = new GenericTransReduce();
    try {
      gtr.close();
    } catch ( NullPointerException ex ) {
      ex.printStackTrace();
      fail( " Null pointer on close look PDI-13080 " + ex.getMessage() );
    }
  }
}
