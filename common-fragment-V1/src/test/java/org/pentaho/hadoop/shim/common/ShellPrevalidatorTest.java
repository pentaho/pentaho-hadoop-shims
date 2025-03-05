/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
