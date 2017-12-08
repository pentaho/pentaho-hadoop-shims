/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format;


import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

public class ParquetUtils {

  public static SchemaDescription createSchema() {
    SchemaDescription s = new SchemaDescription();
    s.addField( s.new Field( "Name", "Name", ValueMetaInterface.TYPE_STRING, true ) );
    s.addField( s.new Field( "Age", "Age", ValueMetaInterface.TYPE_STRING, true ) );
    return s;
  }

  public static SchemaDescription createSchema( int ageType ) {
    SchemaDescription s = new SchemaDescription();
    s.addField( s.new Field( "Name", "Name", ValueMetaInterface.TYPE_STRING, true ) );
    s.addField( s.new Field( "Age", "Age", ageType, true ) );
    return s;
  }

  public static SchemaDescription createSchema( int nameType, boolean nameAllowNull, int ageType,
                                                boolean ageAllowNull ) {
    SchemaDescription s = new SchemaDescription();
    s.addField( s.new Field( "Name", "Name", nameType, nameAllowNull ) );
    s.addField( s.new Field( "Age", "Age", ageType, ageAllowNull ) );
    return s;
  }
}
