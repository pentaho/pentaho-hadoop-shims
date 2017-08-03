package org.pentaho.hadoop.shim.common;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.RecordReader;
import org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat;
import org.pentaho.hadoop.shim.common.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.fs.FileSystemProxy;
import org.pentaho.hdfs.vfs.HadoopFileSystemImpl;

import java.io.IOException;

/**
 * Created by Vasilina_Terehova on 7/27/2017.
 */
public class CommonFormatShimTestIT {

    @Test
    public void testParquetReadSuccessLocalFileSystem() throws IOException, InterruptedException {
        ConfigurationProxy jobConfiguration = new ConfigurationProxy();
        jobConfiguration.set(FileInputFormat.INPUT_DIR, CommonFormatShimTestIT.class.getClassLoader().getResource("sample.pqt").getFile());
        SchemaDescription schemaDescription = makeScheme();
        String schemaString = "message PersonRecord {\n"
                + "required binary name;\n"
                + "required binary age;\n"
                + "}";
        PentahoParquetInputFormat pentahoParquetInputFormat = new PentahoParquetInputFormat(jobConfiguration, schemaDescription,
                new FileSystemProxy(FileSystem.get(jobConfiguration)));
        RecordReader recordReader = pentahoParquetInputFormat.getRecordReader(pentahoParquetInputFormat.getSplits().get(0));
        recordReader.forEach(rowMetaAndData -> {
            RowMetaInterface rowMeta = rowMetaAndData.getRowMeta();
            for (String fieldName : rowMeta.getFieldNames()) {
                try {
                    System.out.println(fieldName + " " + rowMetaAndData.getString(fieldName, ""));
                } catch (KettleValueException e) {
                    e.printStackTrace();
                }
            }
        });

    }

    @Test
    public void testParquetReadSuccessHdfsFileSystem() throws IOException, InterruptedException {
        ConfigurationProxy jobConfiguration = new ConfigurationProxy();
        SchemaDescription schemaDescription = makeScheme();
        jobConfiguration.set(FileInputFormat.INPUT_DIR, "hdfs://svqxbdcn6cdh510n1.pentahoqa.com:8020/user/devuser/parquet");
        PentahoParquetInputFormat pentahoParquetInputFormat = new PentahoParquetInputFormat(jobConfiguration, schemaDescription,
                new FileSystemProxy(new HadoopFileSystemImpl(FileSystem.get(jobConfiguration))));
        RecordReader recordReader = pentahoParquetInputFormat.getRecordReader(pentahoParquetInputFormat.getSplits().get(0));
        recordReader.forEach(rowMetaAndData -> {
            RowMetaInterface rowMeta = rowMetaAndData.getRowMeta();
            for (String fieldName : rowMeta.getFieldNames()) {
                try {
                    System.out.println(fieldName + " " + rowMetaAndData.getString(fieldName, ""));
                } catch (KettleValueException e) {
                    e.printStackTrace();
                }
            }
        });

    }


    @Test
    public void testParquetReadSuccessLocalFileSystemAlex() throws IOException, InterruptedException {
        try {
            SchemaDescription schemaDescription = makeScheme();

            ConfigurationProxy jobConfiguration = new ConfigurationProxy();
            jobConfiguration.set(FileInputFormat.INPUT_DIR, CommonFormatShimTestIT.class.getClassLoader().getResource(
                    "sample.pqt").getFile());
            PentahoParquetInputFormat pentahoParquetInputFormat =
                    new PentahoParquetInputFormat(jobConfiguration, schemaDescription, new FileSystemProxy(FileSystem.get(
                            jobConfiguration)));
            RecordReader recordReader =
                    pentahoParquetInputFormat.getRecordReader(pentahoParquetInputFormat.getSplits().get(0));
            recordReader.forEach(rowMetaAndData -> {
                RowMetaInterface rowMeta = rowMetaAndData.getRowMeta();
                for (String fieldName : rowMeta.getFieldNames()) {
                    try {
                        System.out.println(fieldName + " " + rowMetaAndData.getString(fieldName, ""));
                    } catch (KettleValueException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    private SchemaDescription makeScheme() {
        SchemaDescription s = new SchemaDescription();
        s.addField(s.new Field("Name", "b", ValueMetaInterface.TYPE_STRING));
        s.addField(s.new Field("Age", "c", ValueMetaInterface.TYPE_INTEGER));
        return s;
    }
}
