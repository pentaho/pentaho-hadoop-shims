/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/
package org.pentaho.hadoop.shim.common;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
 * Created by Vasilina_Terehova on 10/3/2017.
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( ShellPrevalidator.class )
public class ShellPrevalidatorTest {

  @Test
  public void validateReturnTrueIfFileExist() throws IOException {
    MemberModifier
      .stub( MemberModifier.method( ShellPrevalidator.class, "doesFileExist", String.class ) )
      .toReturn( true );
    MemberModifier
      .stub( MemberModifier.method( ShellPrevalidator.class, "isWindows" ) )
      .toReturn( true );
    boolean exist = ShellPrevalidator.doesWinutilsFileExist();
    Assert.assertEquals( true, exist );
  }

  @Test( expected = IOException.class )
  public void validateThrowIOExceptionWhenNoExist() throws IOException {
    MemberModifier
      .stub( MemberModifier.method( ShellPrevalidator.class, "doesFileExist", String.class ) )
      .toReturn( false );
    MemberModifier
      .stub( MemberModifier.method( ShellPrevalidator.class, "isWindows" ) )
      .toReturn( true );
    boolean exist = ShellPrevalidator.doesWinutilsFileExist();
    Assert.assertEquals( false, exist );
  }

  @Test( expected = IOException.class )
  public void validateThrowExceptionWhenWindowsAndFileNotExist() throws IOException {
    MemberModifier
      .stub( MemberModifier.method( ShellPrevalidator.class, "isWindows" ) )
      .toReturn( true );
    MemberModifier
      .stub( MemberModifier.method( ShellPrevalidator.class, "doesFileExist", String.class ) )
      .toReturn( false );
    boolean exist = ShellPrevalidator.doesWinutilsFileExist();
    Assert.assertEquals( false, exist );
  }

  @Test
  public void validateReturnTrueIfNotWindows() throws IOException {
    MemberModifier
      .stub( MemberModifier.method( ShellPrevalidator.class, "isWindows" ) )
      .toReturn( false );
    MemberModifier
      .stub( MemberModifier.method( ShellPrevalidator.class, "doesFileExist", String.class ) )
      .toReturn( false );
    boolean exist = ShellPrevalidator.doesWinutilsFileExist();
    Assert.assertEquals( true, exist );
  }

}
