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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Bounds and lifecycle guards for the zstd-jni native codec that Parquet and ORC use for
 * ZSTD compression in the shims.
 *
 * <p>Each guard pins behaviour that only holds from zstd-jni 1.5.7-17 onward. On earlier
 * versions the dictionary constructors accept out-of-range offsets and read native heap
 * memory, the frame-size helpers pass negative or int-overflowing windows straight to the
 * native frame parser, and two of the cases terminate the JVM outright, so these tests also
 * act as regression detectors for a downgrade of the managed zstd-jni version.</p>
 */
public class ZstdCodecBoundsTest {

  private static byte[] sampleDictionary() {
    byte[] dict = new byte[ 64 ];
    for ( int i = 0; i < dict.length; i++ ) {
      dict[ i ] = (byte) i;
    }
    return dict;
  }

  private static byte[] samplePayload() {
    byte[] payload = new byte[ 4096 ];
    for ( int i = 0; i < payload.length; i++ ) {
      payload[ i ] = (byte) ( i % 31 );
    }
    return payload;
  }

  /** The codec must still work normally -- the guards below must not pass by breaking ZSTD. */
  @Test
  public void roundTripsPayloadThroughZstd() {
    byte[] payload = samplePayload();
    byte[] compressed = Zstd.compress( payload, 3 );
    byte[] restored = Zstd.decompress( compressed, payload.length );
    assertArrayEquals( payload, restored );
  }

  /** A dictionary built from a fully in-range window is still accepted and usable. */
  @Test
  public void acceptsInRangeDictionaryWindow() {
    byte[] payload = samplePayload();
    try ( ZstdDictCompress dictCompress = new ZstdDictCompress( sampleDictionary(), 8, 32, 3 );
          ZstdCompressCtx ctx = new ZstdCompressCtx() ) {
      ctx.loadDict( dictCompress );
      byte[] compressed = ctx.compress( payload );
      assertEquals( payload.length, Zstd.decompressedSize( compressed ) );
    }
  }

  /** CVE-2026-87795: length running past the end of the dictionary array must be rejected. */
  @Test
  public void rejectsDictionaryLengthPastEndOfArray() {
    assertRejectedDictionaryWindow( 32, 1024 );
  }

  /** CVE-2026-87795: a negative offset must be rejected rather than read backwards. */
  @Test
  public void rejectsNegativeDictionaryOffset() {
    assertRejectedDictionaryWindow( -8, 16 );
  }

  /** CVE-2026-87795: a negative length must be rejected. */
  @Test
  public void rejectsNegativeDictionaryLength() {
    assertRejectedDictionaryWindow( 0, -1 );
  }

  /** CVE-2026-87795: an offset past the end of the array must be rejected. */
  @Test
  public void rejectsDictionaryOffsetPastEndOfArray() {
    assertRejectedDictionaryWindow( 128, 8 );
  }

  /** CVE-2026-87795: an offset+length pair that overflows int must be rejected. */
  @Test
  public void rejectsOverflowingDictionaryWindow() {
    assertRejectedDictionaryWindow( Integer.MAX_VALUE, 8 );
  }

  private static void assertRejectedDictionaryWindow( int offset, int length ) {
    byte[] dict = sampleDictionary();
    try ( ZstdDictCompress ignored = new ZstdDictCompress( dict, offset, length, 3 ) ) {
      fail( "expected an out-of-range dictionary window (offset=" + offset + ", length=" + length
        + ") to be rejected, but the dictionary was constructed" );
    } catch ( IllegalArgumentException expected ) {
      // zstd-jni >= 1.5.7-14 validates the window instead of reading out of bounds
    }
  }

  /**
   * CVE-2026-87823: the direct-ByteBuffer frame-size native must bounds-check with 64-bit
   * arithmetic. A negative offset near Integer.MIN_VALUE must report an error rather than
   * read unmapped memory.
   */
  @Test
  public void reportsErrorForNegativeDirectBufferFrameOffset() {
    byte[] compressed = Zstd.compress( samplePayload(), 3 );
    ByteBuffer buffer = ByteBuffer.allocateDirect( compressed.length );
    buffer.put( compressed );
    buffer.flip();

    long size = Zstd.getDirectByteBufferFrameContentSize( buffer, Integer.MIN_VALUE + 8, compressed.length );

    assertTrue( "expected an error sentinel for a negative frame offset but got " + size, size < 0 );
  }

  /**
   * CVE-2026-87825: a dictionary that is still referenced by a compression context must not
   * be closable, otherwise later compression reads freed native memory.
   */
  @Test
  public void refusesToCloseDictionaryStillLoadedInAContext() {
    // declared first so try-with-resources closes it last, after the context has released it
    try ( ZstdDictCompress dictCompress = new ZstdDictCompress( sampleDictionary(), 3 );
          ZstdCompressCtx ctx = new ZstdCompressCtx() ) {
      ctx.loadDict( dictCompress );
      try {
        dictCompress.close();
        fail( "expected closing a dictionary that is still loaded in a context to be refused" );
      } catch ( IllegalStateException expected ) {
        // zstd-jni >= 1.5.7-14 holds the shared lock for as long as the context references it
      }

      byte[] payload = samplePayload();
      byte[] compressed = ctx.compress( payload );
      assertEquals( payload.length, Zstd.decompressedSize( compressed ) );
    }
  }

  /** The frame-size helpers must still report the real size for an in-range window. */
  @Test
  @SuppressWarnings( "deprecation" )
  public void reportsFrameContentSizeForInRangeWindow() {
    byte[] payload = samplePayload();
    byte[] compressed = Zstd.compress( payload, 3 );

    assertEquals( payload.length, Zstd.getFrameContentSize( compressed, 0, compressed.length, false ) );
    assertEquals( payload.length, Zstd.decompressedSize( compressed, 0, compressed.length, false ) );
  }

  /** A decompression dictionary built from a fully in-range window is still accepted. */
  @Test
  public void acceptsInRangeDecompressionDictionaryWindow() {
    try ( ZstdDictDecompress dictDecompress = new ZstdDictDecompress( sampleDictionary(), 8, 32 ) ) {
      assertTrue( "expected an in-range decompression dictionary to be constructed", dictDecompress != null );
    }
  }

  /** CVE-2026-90560: length running past the end of the dictionary array must be rejected. */
  @Test
  public void rejectsDecompressionDictionaryLengthPastEndOfArray() {
    assertRejectedDecompressionDictionaryWindow( 32, 1024 );
  }

  /** CVE-2026-90560: a negative offset must be rejected rather than read backwards. */
  @Test
  public void rejectsNegativeDecompressionDictionaryOffset() {
    assertRejectedDecompressionDictionaryWindow( -8, 16 );
  }

  /** CVE-2026-90560: a negative length must be rejected. */
  @Test
  public void rejectsNegativeDecompressionDictionaryLength() {
    assertRejectedDecompressionDictionaryWindow( 0, -1 );
  }

  /** CVE-2026-90560: an offset+length pair that overflows int must be rejected. */
  @Test
  public void rejectsOverflowingDecompressionDictionaryWindow() {
    assertRejectedDecompressionDictionaryWindow( Integer.MAX_VALUE, 8 );
  }

  private static void assertRejectedDecompressionDictionaryWindow( int offset, int length ) {
    byte[] dict = sampleDictionary();
    try ( ZstdDictDecompress ignored = new ZstdDictDecompress( dict, offset, length ) ) {
      fail( "expected an out-of-range decompression dictionary window (offset=" + offset + ", length=" + length
        + ") to be rejected, but the dictionary was constructed" );
    } catch ( IllegalArgumentException expected ) {
      // zstd-jni >= 1.5.7-14 validates the window instead of reading out of bounds
    }
  }

  /** CVE-2026-89046: a negative source position must not reach the native frame parser. */
  @Test
  public void rejectsNegativeFrameContentSizePosition() {
    byte[] compressed = Zstd.compress( samplePayload(), 3 );
    try {
      Zstd.getFrameContentSize( compressed, -8, compressed.length, false );
      fail( "expected a negative source position to be rejected by getFrameContentSize" );
    } catch ( ArrayIndexOutOfBoundsException expected ) {
      // zstd-jni >= 1.5.7-14 rejects a negative srcPosition
    }
  }

  /**
   * The frame-size bounds check must not be defeated by int overflow: srcPosition + srcSize
   * wraps negative and slips past a naive comparison, so the check has to be written as
   * srcSize > src.length - srcPosition. Hardened in zstd-jni 1.5.7-17.
   */
  @Test
  public void rejectsOverflowingFrameContentSizeWindow() {
    byte[] compressed = Zstd.compress( samplePayload(), 3 );
    try {
      Zstd.getFrameContentSize( compressed, 1, Integer.MAX_VALUE, false );
      fail( "expected an int-overflowing frame window to be rejected by getFrameContentSize" );
    } catch ( ArrayIndexOutOfBoundsException expected ) {
      // zstd-jni >= 1.5.7-17 compares without overflowing
    }
  }

  /**
   * XRAY-1081810: the deprecated decompressedSize(byte[],int,int,boolean) overload never
   * validated a negative source position at all, so it handed the offset straight to the
   * native frame parser. Guarded from zstd-jni 1.5.7-17.
   */
  @Test
  @SuppressWarnings( "deprecation" )
  public void rejectsNegativeDecompressedSizePosition() {
    byte[] compressed = Zstd.compress( samplePayload(), 3 );
    try {
      Zstd.decompressedSize( compressed, -8, compressed.length, false );
      fail( "expected a negative source position to be rejected by decompressedSize" );
    } catch ( ArrayIndexOutOfBoundsException expected ) {
      // zstd-jni >= 1.5.7-17 rejects a negative srcPosition
    }
  }

  /** XRAY-1081810: the same overload must also reject an int-overflowing window. */
  @Test
  @SuppressWarnings( "deprecation" )
  public void rejectsOverflowingDecompressedSizeWindow() {
    byte[] compressed = Zstd.compress( samplePayload(), 3 );
    try {
      Zstd.decompressedSize( compressed, 1, Integer.MAX_VALUE, false );
      fail( "expected an int-overflowing frame window to be rejected by decompressedSize" );
    } catch ( ArrayIndexOutOfBoundsException expected ) {
      // zstd-jni >= 1.5.7-17 compares without overflowing
    }
  }
}
