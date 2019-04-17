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

import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MockOutputCollector implements OutputCollector {
  private Map<Object, ArrayList<Object>> collection = new HashMap<Object, ArrayList<Object>>();
  private AtomicBoolean closed = new AtomicBoolean( false );

  public void close() {
    closed.set( true );
  }

  @Override
  public void collect( Object arg0, Object arg1 ) throws IOException {
    if ( closed.get() ) {
      System.out.println( "Already closeds. Nothing could be added." );
      return;
    }
    if ( !collection.containsKey( arg0 ) ) {
      collection.put( arg0, new ArrayList<Object>() );
    }
    collection.get( arg0 ).add( arg1 );
  }

  public Map<Object, ArrayList<Object>> getCollection() {
    return collection;
  }
}
