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

package org.pentaho.hadoop.shim.common;

import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.yarn.api.records.QueueACL;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;

public class YarnQueueAclsVerifier {
  public static boolean verify( QueueAclsInfo[] queueAclsInfos ) throws IOException, InterruptedException {
    return queueAclsInfos != null && Arrays.stream( queueAclsInfos ).map( QueueAclsInfo::getOperations )
      .flatMap( Arrays::stream ).anyMatch( Predicate.isEqual( QueueACL.SUBMIT_APPLICATIONS.toString() ) );
  }
}
