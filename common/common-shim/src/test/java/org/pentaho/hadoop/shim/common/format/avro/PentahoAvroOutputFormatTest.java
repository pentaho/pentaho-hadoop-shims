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
package org.pentaho.hadoop.shim.common.format.avro;

import org.junit.Test;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import static org.junit.Assert.fail;

/**
 * Test the PentahoAvroOutputFormat class
 *
 * Created by jrice on 9/19/2017.
 */
public class PentahoAvroOutputFormatTest {
    @Test
    public void testCreateRecordWriter()  {
        try {
            PentahoAvroOutputFormat avroOutputFormat = new PentahoAvroOutputFormat();
            avroOutputFormat.setCompression(IPentahoAvroOutputFormat.COMPRESSION.DEFLATE);
            avroOutputFormat.setDocValue("documentation for avro schema");
            avroOutputFormat.setNameSpace("org.example");
            avroOutputFormat.setRecordName("AvroOutputRecord");
            avroOutputFormat.setOutputFile("avroOutput");

            // Set up the Avro Schema Description and add fields to it.  Then set that schemaDescription on the avro output format.
            SchemaDescription schemaDescription = new SchemaDescription();
            schemaDescription.addField(schemaDescription.new Field("avroField1", "pentahoField1", 1, true));
            schemaDescription.addField(schemaDescription.new Field("avroField2", "pentahoField2", 1, true));
            avroOutputFormat.setSchemaDescription(schemaDescription);

            IPentahoOutputFormat.IPentahoRecordWriter avroRecordWriter = avroOutputFormat.createRecordWriter();

            Assert.assertNotNull(avroRecordWriter, "avroRecordWriter should NOT be null!");
            Assert.assertTrue(avroRecordWriter instanceof PentahoAvroRecordWriter, "avroRecordWriter should be instance of PentahoAvroRecordWriter");
        }
        catch ( Exception e ) {
            e.printStackTrace();
            fail("test failed due to unexpected exception being thrown:  " + e.getClass() + " - " + e.getMessage());
        }
    }

    @Test
    public void testCreateRecordWriterNegativeNullSchemaDescription() throws Exception {
        PentahoAvroOutputFormat avroOutputFormat = new PentahoAvroOutputFormat();
        avroOutputFormat.setCompression(IPentahoAvroOutputFormat.COMPRESSION.DEFLATE);
        avroOutputFormat.setDocValue("documentation for avro schema");
        avroOutputFormat.setNameSpace("org.example");
        avroOutputFormat.setRecordName("AvroOutputRecord");
        avroOutputFormat.setOutputFile("avroOutput");

        avroOutputFormat.setSchemaDescription(null);

        IPentahoOutputFormat.IPentahoRecordWriter avroRecordWriter = null;
        try {
            avroRecordWriter = avroOutputFormat.createRecordWriter();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof Exception, "Expecting e to be an instance of Exception");
            e.printStackTrace();
            return;
        }

        fail("Exception expected because SchemaDescription is null");
    }

    @Test
    public void testCreateRecordWriterNegativeNullNameSpace() throws Exception {
        PentahoAvroOutputFormat avroOutputFormat = new PentahoAvroOutputFormat();
        avroOutputFormat.setCompression(IPentahoAvroOutputFormat.COMPRESSION.DEFLATE);
        avroOutputFormat.setDocValue("documentation for avro schema");
        avroOutputFormat.setNameSpace(null);
        avroOutputFormat.setRecordName("AvroOutputRecord");
        avroOutputFormat.setOutputFile("avroOutput");

        // Set up the Avro Schema Description and add fields to it.  Then set that schemaDescription on the avro output format.
        SchemaDescription schemaDescription = new SchemaDescription();
        schemaDescription.addField(schemaDescription.new Field("avroField1", "pentahoField1", 1, true));
        schemaDescription.addField(schemaDescription.new Field("avroField2", "pentahoField2", 1, true));
        avroOutputFormat.setSchemaDescription(schemaDescription);

        IPentahoOutputFormat.IPentahoRecordWriter avroRecordWriter = null;

        try {
            avroRecordWriter = avroOutputFormat.createRecordWriter();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof Exception, "Expecting e to be an instance of Exception");
            e.printStackTrace();
            return;
        }

        fail("Exception expected because namespace is null");
    }

    @Test
    public void testCreateRecordWriterNegativeNullRecordName() throws Exception {
        PentahoAvroOutputFormat avroOutputFormat = new PentahoAvroOutputFormat();
        avroOutputFormat.setCompression(IPentahoAvroOutputFormat.COMPRESSION.DEFLATE);
        avroOutputFormat.setDocValue("documentation for avro schema");
        avroOutputFormat.setNameSpace("org.example");
        avroOutputFormat.setRecordName(null);
        avroOutputFormat.setOutputFile("avroOutput");

        // Set up the Avro Schema Description and add fields to it.  Then set that schemaDescription on the avro output format.
        SchemaDescription schemaDescription = new SchemaDescription();
        schemaDescription.addField(schemaDescription.new Field("avroField1", "pentahoField1", 1, true));
        schemaDescription.addField(schemaDescription.new Field("avroField2", "pentahoField2", 1, true));
        avroOutputFormat.setSchemaDescription(schemaDescription);

        IPentahoOutputFormat.IPentahoRecordWriter avroRecordWriter = null;

        try {
            avroRecordWriter = avroOutputFormat.createRecordWriter();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof Exception, "Expecting e to be an instance of Exception");
            e.printStackTrace();
            return;
        }

        fail("Exception expected because recordName is null");
    }
}
