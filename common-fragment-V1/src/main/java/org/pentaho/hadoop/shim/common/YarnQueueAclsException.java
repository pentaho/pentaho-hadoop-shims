/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common;

public class YarnQueueAclsException extends RuntimeException {
  public YarnQueueAclsException() {
    super();
  }

  public YarnQueueAclsException( String message ) {
    super( message );
  }

  public YarnQueueAclsException( String message, Throwable cause ) {
    super( message, cause );
  }

  public YarnQueueAclsException( Throwable cause ) {
    super( cause );
  }

  protected YarnQueueAclsException( String message, Throwable cause,
                                    boolean enableSuppression,
                                    boolean writableStackTrace ) {
    super( message, cause, enableSuppression, writableStackTrace );
  }
}
