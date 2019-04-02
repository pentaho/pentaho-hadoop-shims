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
