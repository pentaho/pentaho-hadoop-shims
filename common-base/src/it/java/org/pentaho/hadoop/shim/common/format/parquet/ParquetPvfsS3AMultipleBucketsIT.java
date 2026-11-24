package org.pentaho.hadoop.shim.common.format.parquet;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import com.google.common.cache.Cache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat.IPentahoRecordWriter;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheOutputFormat;
import org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge;

import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * PDI-19753: actual Parquet -> PVFS Hadoop bridge -> S3A -> isolated MinIO.
 * Only connection lookup is stubbed; path mapping, cache keys, S3A path checks,
 * Parquet writers and object IO are production implementations.
 */
public class ParquetPvfsS3AMultipleBucketsIT {
  private static final String ACCESS = "offline-test-access";
  private static final String SECRET = "offline-test-secret";
  private static final String OBJECT = "output.parquet";

  @Rule public TemporaryFolder temporary = new TemporaryFolder();
  private String containerId;
  private String endpoint;
  private S3Client s3;
  private final Set<FileSystem> bridges = new HashSet<>();

  @Before
  public void startOwnedMinio() throws Exception {
    KettleLogStore.init();
    containerId = command( "docker", "run", "--rm", "-d",
      "--name", "pdi21063-parquet-" + UUID.randomUUID(),
      "-p", "127.0.0.1::9000", "-e", "MINIO_ROOT_USER=" + ACCESS,
      "-e", "MINIO_ROOT_PASSWORD=" + SECRET,
      "quay.io/minio/minio:latest", "server", "/data" ).trim();
    assertTrue( "Docker must return an exact container ID", containerId.matches( "[a-f0-9]{64}" ) );
    String port = command( "docker", "port", containerId, "9000/tcp" ).trim();
    assertTrue( "MinIO must only be published on loopback", port.startsWith( "127.0.0.1:" ) );
    endpoint = "http://" + port;
    awaitReady();
    s3 = S3Client.builder()
      .credentialsProvider( StaticCredentialsProvider.create( AwsBasicCredentials.create( ACCESS, SECRET ) ) )
      .endpointOverride( URI.create( endpoint ) ).region( Region.US_EAST_1 )
      .serviceConfiguration( S3Configuration.builder().pathStyleAccessEnabled( true ).build() ).build();
  }

  @After
  public void closeOnlyOwnedResources() throws Exception {
    try {
      // PvfsHadoopBridge.close() does not close its private inner filesystem
      // cache. Close those exact owned S3A clients, never FileSystem.closeAll().
      for ( FileSystem bridge : bridges ) {
        try {
          Field field = PvfsHadoopBridge.class.getDeclaredField( "fsCache" );
          field.setAccessible( true );
          Cache<?, ?> cache = (Cache<?, ?>) field.get( bridge );
          for ( Object fs : new HashSet<>( cache.asMap().values() ) ) {
            ( (FileSystem) fs ).close();
          }
          cache.invalidateAll();
        } finally {
          bridge.close();
        }
      }
    } finally {
      try {
        if ( s3 != null ) {
          s3.close();
        }
      } finally {
        if ( containerId != null && containerId.matches( "[a-f0-9]{64}" ) ) {
          command( "docker", "rm", "-f", containerId );
        }
      }
    }
  }

  @Test
  public void parquetOutputToSingleBucketWorks() throws Exception {
    exercise( false );
  }

  @Test
  public void onePvfsConnectionWritesParquetToTwoBuckets() throws Exception {
    exercise( true );
  }

  private void exercise( boolean secondBucket ) throws Exception {
    String suffix = UUID.randomUUID().toString();
    String connection = "parquet-" + suffix;
    String bucketA = "bucket-a-" + suffix;
    String bucketB = "bucket-b-" + suffix;
    s3.createBucket( request -> request.bucket( bucketA ) );
    if ( secondBucket ) {
      s3.createBucket( request -> request.bucket( bucketB ) );
    }
    Map<String, String> properties = new HashMap<>();
    properties.put( "accessKey", ACCESS );
    properties.put( "secretKey", SECRET );
    properties.put( "endpoint", endpoint );
    properties.put( "pathStyleAccess", "true" );
    ConnectionDetails details = mock( ConnectionDetails.class );
    when( details.getType() ).thenReturn( "s3" );
    when( details.getProperties() ).thenReturn( properties );
    ConnectionManager manager = mock( ConnectionManager.class );
    when( manager.getConnectionDetails( connection ) ).thenReturn( details );
    // No default S3 connection: exercise only the explicitly named PVFS one.
    when( manager.getConnectionDetailsByScheme( anyString() ) ).thenReturn( new ArrayList<>() );
    try ( MockedStatic<ConnectionManager> lookup = mockStatic( ConnectionManager.class ) ) {
      lookup.when( ConnectionManager::getInstance ).thenReturn( manager );
      writeAndVerifyParquet( connection, bucketA );
      if ( secondBucket ) {
        writeAndVerifyParquet( connection, bucketB );
      }
    }
  }

  private void writeAndVerifyParquet( String connection, String bucket ) throws Exception {
    Output format = new Output();
    String path = "pvfs://" + connection + "/" + bucket + "/" + OBJECT;
    bridges.add( FileSystem.get( URI.create( path ), format.configuration() ) );
    ParquetOutputField field = new ParquetOutputField();
    field.setFormatFieldName( "value" );
    field.setPentahoFieldName( "value" );
    field.setFormatType( ParquetSpec.DataType.UTF8 );
    field.setAllowNull( false );
    format.setFields( singletonList( field ) );
    format.setCompression( CompressionCodecName.UNCOMPRESSED );
    format.setOutputFile( path, false );
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "value" ) );
    IPentahoRecordWriter writer = format.createRecordWriter();
    try {
      writer.write( new RowMetaAndData( rowMeta, new Object[] { bucket } ) );
    } finally {
      writer.close();
    }
    byte[] bytes = s3.getObjectAsBytes( request -> request.bucket( bucket ).key( OBJECT ) ).asByteArray();
    assertTrue( "A completed Parquet object must exist in " + bucket, bytes.length > 8 );
    assertEquals( "PAR1", new String( bytes, 0, 4, StandardCharsets.US_ASCII ) );
    assertEquals( "PAR1", new String( bytes, bytes.length - 4, 4, StandardCharsets.US_ASCII ) );
    java.nio.file.Path local = temporary.newFile().toPath();
    Files.write( local, bytes );
    assertEquals( "Parquet footer must contain the written row in " + bucket, 1L,
      ParquetFileReader.readFooter( new Configuration(),
        new org.apache.hadoop.fs.Path( local.toUri() ) ).getBlocks().stream()
        .mapToLong( block -> block.getRowCount() ).sum() );
  }

  /** Exposes configuration only; all writer methods are inherited unchanged. */
  private static class Output extends PentahoApacheOutputFormat {
    Configuration configuration() {
      return job.getConfiguration();
    }
  }

  private void awaitReady() throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos( 45 );
    while ( System.nanoTime() < deadline ) {
      HttpURLConnection connection = (HttpURLConnection) new URL( endpoint + "/minio/health/ready" ).openConnection();
      connection.setConnectTimeout( 500 );
      connection.setReadTimeout( 500 );
      try {
        if ( connection.getResponseCode() == 200 ) {
          return;
        }
      } catch ( java.io.IOException notReady ) {
        // Poll an observable readiness condition, not a startup sleep.
      } finally {
        connection.disconnect();
      }
      Thread.sleep( 100 );
    }
    throw new IllegalStateException( "Owned MinIO did not become ready: " + containerId );
  }

  private static String command( String... arguments ) throws Exception {
    Process process = new ProcessBuilder( arguments ).redirectErrorStream( true ).start();
    try {
      if ( !process.waitFor( 60, TimeUnit.SECONDS ) ) {
        throw new IllegalStateException( "Timed out: " + arguments[ 0 ] );
      }
      String output = new String( process.getInputStream().readAllBytes(), StandardCharsets.UTF_8 );
      if ( process.exitValue() != 0 ) {
        throw new IllegalStateException( arguments[ 0 ] + " failed: " + output );
      }
      return output;
    } finally {
      process.destroy();
    }
  }
}
