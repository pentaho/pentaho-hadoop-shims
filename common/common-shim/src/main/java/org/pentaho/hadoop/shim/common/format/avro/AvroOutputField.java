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

package org.pentaho.hadoop.shim.common.format.avro;

import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroOutputField;
import org.pentaho.hadoop.shim.common.format.BaseFormatOutputField;

/**
 * Base class for format's input/output field - path added.
 * 
 * @author JRice <joseph.rice@hitachivantara.com>
 */
public class AvroOutputField extends BaseFormatOutputField implements IAvroOutputField {
  @Override
  public AvroSpec.DataType getAvroType() {
    return AvroSpec.DataType.values()[ formatType ];
  }

  @Override
  public void setFormatType( AvroSpec.DataType avroType ) {
    this.formatType = avroType.ordinal();
  }

  @Override
  public void setFormatType( int formatType ) {
    this.formatType = AvroSpec.DataType.values()[ formatType ].ordinal();
  }

  @Override
  public int getFormatType() {
    return formatType;
  }
}
