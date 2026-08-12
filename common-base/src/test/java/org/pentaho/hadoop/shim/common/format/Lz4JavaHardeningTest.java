/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.hadoop.shim.common.format;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4DecompressorWithLength;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Guards the behaviour of the bundled LZ4 codec ({@code at.yawk.lz4:lz4-java}) that the ORC and
 * Parquet formats in this module compress with. Malformed or hostile input must surface as a normal
 * Java exception; it must never read outside a caller-supplied array range and must never be trusted
 * to size an allocation from an attacker-controlled length header.
 */
public class Lz4JavaHardeningTest {

  private static final byte[] EIGHT_BYTES = new byte[ 8 ];

  @Test
  public void nativeStreamingHash32RejectsAnOutOfBoundsRange() {
    StreamingXXHash32 hash = XXHashFactory.nativeInstance().newStreamingHash32( 0x9747b28c );
    try {
      hash.update( EIGHT_BYTES, 100, 100 );
      fail( "native streaming XXHash32 accepted a range outside the array instead of rejecting it" );
    } catch ( ArrayIndexOutOfBoundsException expected ) {
      // the range is validated in Java before it reaches the native code
    }
  }

  @Test
  public void nativeStreamingHash64RejectsAnOutOfBoundsRange() {
    StreamingXXHash64 hash = XXHashFactory.nativeInstance().newStreamingHash64( 0x9747b28cL );
    try {
      hash.update( EIGHT_BYTES, 100, 100 );
      fail( "native streaming XXHash64 accepted a range outside the array instead of rejecting it" );
    } catch ( ArrayIndexOutOfBoundsException expected ) {
      // the range is validated in Java before it reaches the native code
    }
  }

  @Test
  public void nativeStreamingHash32RejectsANegativeOffset() {
    StreamingXXHash32 hash = XXHashFactory.nativeInstance().newStreamingHash32( 0 );
    try {
      hash.update( EIGHT_BYTES, -1, 4 );
      fail( "native streaming XXHash32 accepted a negative offset" );
    } catch ( ArrayIndexOutOfBoundsException expected ) {
      // expected
    }
  }

  @Test
  public void safeAndNativeStreamingHash32AgreeOnValidInput() {
    byte[] data = payload( 512 );
    assertEquals( streamingHash32( XXHashFactory.safeInstance(), data ),
      streamingHash32( XXHashFactory.nativeInstance(), data ) );
  }

  @Test
  public void oneShotHashRejectsAnOutOfBoundsRange() {
    try {
      XXHashFactory.nativeInstance().hash32().hash( EIGHT_BYTES, 100, 100, 0 );
      fail( "one-shot XXHash32 accepted a range outside the array" );
    } catch ( ArrayIndexOutOfBoundsException expected ) {
      // expected
    }
  }

  @Test
  public void decompressorWithLengthRejectsAHostileLengthHeader() {
    // a 4-byte little-endian length header claiming ~2 GiB in front of 4 bytes of payload
    byte[] hostile = new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F, 1, 2, 3, 4 };
    LZ4DecompressorWithLength decompressor =
      new LZ4DecompressorWithLength( LZ4Factory.fastestInstance().safeDecompressor() );
    try {
      decompressor.decompress( hostile );
      fail( "decompressorWithLength accepted a length header that the input cannot support" );
    } catch ( LZ4Exception expected ) {
      // the declared length is validated against the actual input before any allocation
    }
  }

  @Test
  public void blockInputStreamRejectsACorruptedHeader() throws IOException {
    ByteArrayOutputStream raw = new ByteArrayOutputStream();
    raw.write( new byte[] { 'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k' } );
    raw.write( 0x20 | 0x0F );
    writeLittleEndian( raw, Integer.MAX_VALUE - 8 );
    writeLittleEndian( raw, Integer.MAX_VALUE - 8 );
    writeLittleEndian( raw, 0 );
    raw.write( new byte[] { 1, 2, 3, 4 } );

    try ( LZ4BlockInputStream in = new LZ4BlockInputStream( new ByteArrayInputStream( raw.toByteArray() ) ) ) {
      in.read();
      fail( "LZ4BlockInputStream accepted a corrupted block header" );
    } catch ( IOException expected ) {
      assertNotNull( expected.getMessage() );
    }
  }

  @Test
  public void roundTripStillWorksAfterTheHardening() {
    byte[] source = payload( 64 * 1024 );
    LZ4Factory factory = LZ4Factory.fastestInstance();
    byte[] compressed = factory.fastCompressor().compress( source );
    byte[] restored = factory.fastDecompressor().decompress( compressed, source.length );
    assertArrayEquals( source, restored );
  }

  @Test
  public void roundTripWithLengthStillWorksAfterTheHardening() {
    byte[] source = payload( 4096 );
    LZ4Factory factory = LZ4Factory.fastestInstance();
    byte[] compressed = factory.fastCompressor().compress( source );
    byte[] withLength = new byte[ compressed.length + 4 ];
    withLength[ 0 ] = (byte) ( source.length & 0xFF );
    withLength[ 1 ] = (byte) ( ( source.length >>> 8 ) & 0xFF );
    withLength[ 2 ] = (byte) ( ( source.length >>> 16 ) & 0xFF );
    withLength[ 3 ] = (byte) ( ( source.length >>> 24 ) & 0xFF );
    System.arraycopy( compressed, 0, withLength, 4, compressed.length );

    byte[] restored =
      new LZ4DecompressorWithLength( factory.safeDecompressor() ).decompress( withLength );
    assertArrayEquals( source, restored );
  }

  private static int streamingHash32( XXHashFactory factory, byte[] data ) {
    StreamingXXHash32 hash = factory.newStreamingHash32( 0x9747b28c );
    hash.update( data, 0, data.length );
    return hash.getValue();
  }

  private static byte[] payload( int size ) {
    byte[] data = new byte[ size ];
    for ( int i = 0; i < size; i++ ) {
      data[ i ] = (byte) ( i % 7 );
    }
    return Arrays.copyOf( data, size );
  }

  private static void writeLittleEndian( OutputStream out, int value ) throws IOException {
    out.write( value & 0xFF );
    out.write( ( value >>> 8 ) & 0xFF );
    out.write( ( value >>> 16 ) & 0xFF );
    out.write( ( value >>> 24 ) & 0xFF );
  }
}
