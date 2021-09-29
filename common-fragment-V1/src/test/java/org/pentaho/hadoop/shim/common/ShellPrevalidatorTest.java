/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2019-2021 by Hitachi Vantara : http://www.pentaho.com
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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyString;

/**
 * Created by Vasilina_Terehova on 10/3/2017.
 */
@RunWith( MockitoJUnitRunner.class )
public class ShellPrevalidatorTest {

  MockedStatic<ShellPrevalidator> shellPrevalidatorMockedStatic;

  @Before
  public void setup() {
    shellPrevalidatorMockedStatic = Mockito.mockStatic( ShellPrevalidator.class );
  }

  @After
  public void cleanup() {
    shellPrevalidatorMockedStatic.close();
  }

  @Test
  public void validateReturnTrueIfFileExist() throws IOException {
    shellPrevalidatorMockedStatic.when( () -> ShellPrevalidator.doesFileExist( anyString() ) ).thenReturn( true );
    shellPrevalidatorMockedStatic.when( ShellPrevalidator::isWindows ).thenReturn( true );
    shellPrevalidatorMockedStatic.when( ShellPrevalidator::doesWinutilsFileExist ).thenCallRealMethod();

    boolean exist = ShellPrevalidator.doesWinutilsFileExist();
    Assert.assertEquals( true, exist );
  }

  @Test( expected = IOException.class )
  public void validateThrowIOExceptionWhenNoExist() throws IOException {
    shellPrevalidatorMockedStatic.when( () -> ShellPrevalidator.doesFileExist( anyString() ) ).thenReturn( false );
    shellPrevalidatorMockedStatic.when( ShellPrevalidator::isWindows ).thenReturn( true );
    shellPrevalidatorMockedStatic.when( ShellPrevalidator::doesWinutilsFileExist ).thenCallRealMethod();
    boolean exist = ShellPrevalidator.doesWinutilsFileExist();
    Assert.assertEquals( false, exist );
  }

  @Test
  public void validateThrowExceptionWhenWindowsAndFileNotExist() throws IOException {
    shellPrevalidatorMockedStatic.when( () -> ShellPrevalidator.doesFileExist( anyString() ) ).thenReturn( true );
    shellPrevalidatorMockedStatic.when( ShellPrevalidator::isWindows ).thenReturn( false );
    shellPrevalidatorMockedStatic.when( ShellPrevalidator::doesWinutilsFileExist ).thenCallRealMethod();
    boolean exist = ShellPrevalidator.doesWinutilsFileExist();
    Assert.assertEquals( true, exist );
  }

  @Test
  public void validateReturnTrueIfNotWindows() throws IOException {
    shellPrevalidatorMockedStatic.when( () -> ShellPrevalidator.doesFileExist( anyString() ) ).thenReturn( false );
    shellPrevalidatorMockedStatic.when( ShellPrevalidator::isWindows ).thenReturn( false );
    shellPrevalidatorMockedStatic.when( ShellPrevalidator::doesWinutilsFileExist ).thenCallRealMethod();
    boolean exist = ShellPrevalidator.doesWinutilsFileExist();
    Assert.assertEquals( true, exist );
  }

}
