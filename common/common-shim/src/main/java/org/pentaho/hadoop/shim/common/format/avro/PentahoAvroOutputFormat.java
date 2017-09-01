/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.avro;

import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

/**
 * 
 * @author Alexander Buloichik
 */
public class PentahoAvroOutputFormat implements IPentahoAvroOutputFormat {

  @Override
  public IPentahoRecordWriter createRecordWriter() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setSchema( SchemaDescription schema ) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void setOutputFile( String file ) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void setCompression( COMPRESSION compression ) {
    // TODO Auto-generated method stub

  }

}
