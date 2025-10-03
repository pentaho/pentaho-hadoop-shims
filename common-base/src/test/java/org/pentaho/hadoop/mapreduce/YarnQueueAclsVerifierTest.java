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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.junit.Test;
import org.pentaho.hadoop.shim.common.YarnQueueAclsVerifier;

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
