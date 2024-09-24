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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class MockRecordReader implements RecordReader<Text, Text> {
  private Iterator<String> rowIter;
  int rowNum = -1;
  int totalRows;

  // Make them provide a pre-filled list so we don't confuse the overhead of generating strings
  // with the time it takes to run the mapper
  public MockRecordReader( List<String> rows ) {
    totalRows = rows.size();
    rowIter = rows.iterator();
  }

  @Override
  public boolean next( Text key, Text value ) throws IOException {
    if ( !rowIter.hasNext() ) {
      return false;
    }
    rowNum++;
    key.set( String.valueOf( rowNum ) );
    value.set( rowIter.next() );
    return true;
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public Text createValue() {
    return new Text();
  }

  @Override
  public long getPos() throws IOException {
    return rowNum;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public float getProgress() throws IOException {
    return ( rowNum + 1 ) / totalRows;
  }
}
