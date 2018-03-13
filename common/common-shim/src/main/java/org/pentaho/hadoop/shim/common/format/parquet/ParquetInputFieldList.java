/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.parquet;

import org.pentaho.hadoop.shim.api.format.IParquetInputField;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ParquetInputFieldList implements Iterable<IParquetInputField> {

  private List<IParquetInputField> fields;

  public ParquetInputFieldList( List<IParquetInputField> fields ) {
    this.fields = fields;
  }

  public List<IParquetInputField> getFields() {
    return this.fields;
  }

  public String marshall() {
    StringBuilder o = new StringBuilder( 2048 );
    for ( IParquetInputField f : fields ) {
      o.append( IParquetInputField.marshall( f ) ).append( '\n' );
    }
    return o.toString();
  }

  public static ParquetInputFieldList unmarshall( String str ) {
    List<IParquetInputField> inputFields = new ArrayList<>();
    if ( str.isEmpty() ) {
      return new ParquetInputFieldList( inputFields );
    }
    String[] lines = str.split( "\n" );
    for ( String line : lines ) {
      inputFields.add( ParquetInputField.unmarshallField( line ) );
    }
    return new ParquetInputFieldList( inputFields );
  }

  public boolean isEmpty() {
    return fields.isEmpty();
  }

  public int getFieldsCount() {
    return fields.size();
  }

  @Override
  public Iterator<IParquetInputField> iterator() {
    return fields.iterator();
  }

  @Override
  public String toString() {
    return marshall();
  }
}
