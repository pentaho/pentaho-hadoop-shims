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


package org.apache.hadoop.mapred;

import java.util.ArrayList;

/**
 * This is an empty, fake class necessary for all non-MapR shims because MapR's Hadoop adds a method to the RunningJob
 * interface that returns a class that only exists in the MapR Hadoop distribution. So we add a fake one here so the
 * common tests will compile and run successfully.
 */
public class TaskCompletionEventList extends ArrayList<TaskCompletionEvent> {

}
