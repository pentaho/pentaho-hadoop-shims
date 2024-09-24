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
package org.pentaho.hadoop.mapreduce;

import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

public class MockOutputCollector implements OutputCollector {

  private static LogChannelInterface log = new LogChannel( MockOutputCollector.class.getName() );

  private Map<Object, ArrayList<Object>> collection = new HashMap<Object, ArrayList<Object>>();
  private AtomicBoolean closed = new AtomicBoolean( false );

  public void close() {
    closed.set( true );
  }

  @Override
  public void collect( Object arg0, Object arg1 ) throws IOException {
    if ( closed.get() ) {
      log.logBasic( "Already closeds. Nothing could be added." );
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
