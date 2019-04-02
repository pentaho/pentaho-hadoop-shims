/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.hadoop.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class YarnQueueAclsVerifierTest {
  @Test
  public void testVerifyWhenUserHasNoPermissionsForSubmitInAnyQueueShouldReturnFalse() throws Exception {
    assertFalse( YarnQueueAclsVerifier.verify( new QueueAclsInfo[] {
      new QueueAclsInfo( StringUtils.EMPTY, new String[] {
        "ANOTHER_RIGHTS"
      } ),
      new QueueAclsInfo( StringUtils.EMPTY, new String[] {} )
    } ) );
  }

  @Test
  public void testVerifyWhenUserHasPermissionsForSubmitInAnyQueueShouldReturnTrue() throws Exception {
    assertTrue( YarnQueueAclsVerifier.verify( new QueueAclsInfo[] {
      new QueueAclsInfo( StringUtils.EMPTY, new String[] {
        "SUBMIT_APPLICATIONS"
      } ),
      new QueueAclsInfo( StringUtils.EMPTY, new String[] {} )
    } ) );
  }

  @Test
  public void testVerifyWhenQueueAclsArrayIsNullShouldReturnFalse() throws Exception {
    assertFalse( YarnQueueAclsVerifier.verify( null ) );
  }
}