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
package org.apache.hadoop.mapred;

public class MockQueueAclsInfo extends QueueAclsInfo {
  public MockQueueAclsInfo( String queueName, String[] operations ) {
    super( queueName, operations );
  }
}
