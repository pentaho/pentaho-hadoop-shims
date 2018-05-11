/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.apache.hadoop.mapred;

import java.util.ArrayList;

/**
 * This is an empty, fake class necessary for all non-MapR shims because MapR's Hadoop adds a method to the RunningJob
 * interface that returns a class that only exists in the MapR Hadoop distribution. So we add a fake one here so the
 * common tests will compile and run successfully.
 */
public class TaskCompletionEventList extends ArrayList<TaskCompletionEvent> {

}
