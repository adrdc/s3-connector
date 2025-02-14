/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.s3.integration;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.s3.S3Const;
import com.facebook.presto.s3.services.EmbeddedSchemaRegistry;
import com.facebook.presto.s3.util.SimpleS3Server;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.log.Level.DEBUG;
import static com.facebook.presto.s3.integration.S3QueryRunner.createQueryRunner;
import static org.testng.Assert.*;

@Test
public class S3QueryTest {
    private final boolean scality = false;

    private Process p1 = null;
    private QueryRunner queryRunner;
    private static final Logger log = Logger.get(S3QueryTest.class);

    private EmbeddedSchemaRegistry schemaRegistry;

    private SimpleS3Server s3Server;

    @BeforeSuite
    public void setUp()
            throws Exception {

        if (scality) {
            try {
                String[] cmd = {"bash", "src/test/bin/s3_start.sh"};
                System.out.println("Start s3 server and load data");
                this.p1 = Runtime.getRuntime().exec(cmd);

                BufferedReader output = new BufferedReader(new InputStreamReader(p1.getInputStream()));
                String cmdOut = output.readLine();
                while (cmdOut != null) {
                    System.out.println(cmdOut);
                    cmdOut = output.readLine();
                }
            } catch (Exception e) {
                throw new Exception("Exception starting s3 server: " + e.toString());
            }
            p1.waitFor();
            if (p1.exitValue() == 0) {
                System.out.println("s3 server started and data loaded");
            } else {
                throw new Exception("s3 server failed to start");
            }
        } else {
            s3Server = new SimpleS3Server(8000);
            s3Server.start();

            putToS3Server("testbucket", "names.csv", "names.csv");
            putToS3Server("testbucket", "grades/grades.csv", "grades.csv");
            putToS3Server("testbucket", "cartoondb/cartoon_table.json", "json_datafile");
            putToS3Server("testbucket", "jsondata/json_datafile", "json_datafile");
            putToS3Server("testbucket", "datafile.txt", "datafile.txt");
            putToS3Server("testbucket", "customer/customerfile", "customerfile");
            putToS3Server("testbucket", "store/storefile", "storefile");
            putToS3Server("testbucket", "avro_datafile", "avro_datafile");
            putToS3Server("testbucket", "medical.csv.gz", "medical.csv.gz");
            putToS3Server("testbucket", "types.json.gz", "types.json.gz");
        }

        schemaRegistry = new EmbeddedSchemaRegistry();
        schemaRegistry.start();
        System.out.println("schema registry server started");

        Map<String, String> extraProps = ImmutableMap.<String, String>builder()
                                                     .put("s3.schemaRegistryPort", String.valueOf(schemaRegistry.port()))
                                                     .build();
        queryRunner = createQueryRunner(extraProps);

        Logging logging = Logging.initialize();
        logging.setLevel("com.facebook.presto.s3", DEBUG);
    }

    void putToS3Server(String bucket, String key, String file) {
        File f = new File(S3QueryTest.class.getResource("/" + file).getFile());
        s3Server.putKey(bucket, key, f);
    }

    @Test
    public void resetSchema() {
        log.info("Test: resetSchema");
        try {
            // Drop a bogus schema - this will cause a reset of the s3 catalog schemas
            queryRunner.execute("DROP SCHEMA s3.bogus");
        } catch (Exception e) {
            // Ignore this
        }

        assertEquals(true, true);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testShowSchema() {
        log.info("Test: testShowSchema");
        assertEquals(queryRunner.execute("SHOW SCHEMAS FROM s3").getRowCount(), 8);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testDescribeTable() {
        log.info("Test: testDescribeTable");
        assertEquals(queryRunner.execute("DESCRIBE s3.studentdb.medical").getRowCount(), 3);
    }

    @Test(dependsOnMethods = "resetSchema",
            expectedExceptions = {RuntimeException.class},
            expectedExceptionsMessageRegExp = ".*MetaData Search is not Enabled for this Bucket.*")
    public void testDescribeBucketTable() {
        log.info("Test: testDescribeBucketTable");
        log.info("S3_BUCKETS TABLES" + queryRunner.execute("SHOW TABLES FROM s3.s3_buckets").getMaterializedRows().toString());
        assertEquals(queryRunner.execute("DESCRIBE s3.s3_buckets.testbucket").getRowCount(), 3);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testShowTables() {
        log.info("Test: testShowTables");
        assertEquals(queryRunner.execute("SHOW TABLES FROM s3.studentdb").getRowCount(), 3);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectStarFromCsvTable() {
        log.info("Test: testSelectStarFromCsvTable");
        assertFalse(s3SelectEnabledForSession(queryRunner.getDefaultSession()));
        assertEquals(queryRunner.execute("SELECT * FROM s3.studentdb.medical").getRowCount(), 6);
    }

    @Test
    public void testSelectStarFromJSONTable() {
        log.info("Test: testSelectStarFromJSONTable");
        assertEquals(queryRunner.execute("SELECT * FROM s3.cartoondb.addressTable").getRowCount(), 3);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectStarFromAvroTable() {
        log.info("Test: testSelectStarFromAvroTable");
        assertEquals(queryRunner.execute("SELECT * FROM s3.olympicdb.medaldatatable").getRowCount(), 100);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectStarFromParquetTable() {
        log.info("Test: testSelectStarFromParquetTable");
        assertEquals(queryRunner.execute("SELECT * FROM s3.parquetdata.customer").getRowCount(), 144000);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testIndividualColumnParquetTable() {
        log.info("Test: testIndividualColumnParquetTable");
        assertEquals(queryRunner.execute("SELECT s_tax_percentage FROM s3.parquetdata.store").getRowCount(), 22);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testShowTablesParquet() {
        log.info("Test: testShowTablesParquet");
        assertEquals(queryRunner.execute("SHOW tables FROM s3.parquetdata").getRowCount(), 2);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testDescribeTablesParquet() {
        log.info("Test: testDescribeTablesParquet");
        assertEquals(queryRunner.execute("DESCRIBE s3.parquetdata.customer").getRowCount(), 18);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectStarWhereClauseParquet() {
        log.info("Test: testSelectStarWhereClauseParquet");
        assertEquals(queryRunner.execute("SELECT * FROM s3.parquetdata.store where s_rec_start_date=9933").getRowCount(), 12);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectWhereClauseSingleColumnsParquet() {
        log.info("Test: testSelectWhereClauseSingleColumnsParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id FROM s3.parquetdata.store where s_rec_start_date=9933").getRowCount(), 12);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectWhereClauseMultipleColumnsParquet() {
        log.info("Test: testSelectWhereClauseMultipleColumnsParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id,s_rec_start_date FROM s3.parquetdata.store where s_rec_start_date=9933").getRowCount(), 12);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectStarMultipleWhereClausesParquet() {
        log.info("Test: testSelectStarMultipleWhereClausesParquet");
        assertEquals(queryRunner.execute("SELECT * FROM s3.parquetdata.store where s_rec_start_date=9933 and s_rec_end_date=11028").getRowCount(), 4);
    }

    @Test
    public void testSelectMultipleWhereClausesSingleColumnParquet() {
        log.info("Test: testSelectMultipleWhereClausesSingleColumnParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id FROM s3.parquetdata.store where s_rec_start_date=9933 and s_rec_end_date=11028").getRowCount(), 4);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectMultipleWhereClausesMultipleColumnsParquet() {
        log.info("Test: testSelectMultipleWhereClausesSingleColumnParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id,s_rec_start_date,s_rec_end_date FROM s3.parquetdata.store where s_rec_start_date=9933 and s_rec_end_date=11028").getRowCount(), 4);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectStarWhereClauseWithOperatorsParquet() {
        log.info("Test: testSelectStarWhereClauseWithOperatorsParquet");
        assertEquals(queryRunner.execute("SELECT * FROM s3.parquetdata.store where s_rec_start_date>10000").getRowCount(), 10);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectWhereClauseWithOperatorsParquet() {
        log.info("Test: testSelectWhereClauseWithOperatorsParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id FROM s3.parquetdata.store where s_rec_start_date>10000").getRowCount(), 10);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectWhereClauseWithOperatorsMultipleColumnsParquet() {
        log.info("Test: testSelectWhereClauseWithOperatorsParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id,s_rec_start_date FROM s3.parquetdata.store where s_rec_start_date>10000").getRowCount(), 10);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectMultipleWhereClauseWithOperatorsMultipleColumnsParquet() {
        log.info("Test: testSelectMultipleWhereClauseWithOperatorsMultipleColumnsParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id,s_rec_start_date FROM s3.parquetdata.store where s_rec_start_date>10000 and s_rec_end_date>10664").getRowCount(), 3);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectStarWithNoEntriesParquet() {
        log.info("Test: testSelectStarWithNoEntriesParquet");
        assertEquals(queryRunner.execute("SELECT * FROM s3.parquetdata.store where s_rec_start_date>100000").getRowCount(), 0);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectSingleColumnWithNoEntriesParquet() {
        log.info("Test: testSelectSingleColumnWithNoEntriesParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id FROM s3.parquetdata.store where s_rec_start_date>100000").getRowCount(), 0);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSelectMultipleColumnsWithNoEntriesParquet() {
        log.info("Test: testSelectMultipleColumnsWithNoEntriesParquet");
        assertEquals(queryRunner.execute("SELECT s_store_id,s_rec_start_date FROM s3.parquetdata.store where s_rec_start_date>100000").getRowCount(), 0);
    }

    @Test(dependsOnMethods = "testShowSchema")
    public void testCreateSchema() {
        log.info("Test: testCreateSchema");

        int SchemaCountBefore = queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount();
        queryRunner.execute("CREATE SCHEMA s3.newschema");
        log.info(queryRunner.execute("SHOW SCHEMAS IN s3").getMaterializedRows().toString());
        assertEquals(SchemaCountBefore + 1, queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount());
    }

    @Test(dependsOnMethods = "testDropTable")
    public void testDropSchema() {
        log.info("Test: testDropSchema");

        int SchemaCountBefore = queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount();
        queryRunner.execute("DROP SCHEMA s3.newschema");
        log.info(queryRunner.execute("SHOW SCHEMAS IN s3").getMaterializedRows().toString());
        assertEquals(SchemaCountBefore - 1, queryRunner.execute("SHOW SCHEMAS IN s3").getRowCount());
    }

    @Test(dependsOnMethods = "testCreateSchema")
    public void testCreateTable() {
        log.info("Test: testCreateTable");
        queryRunner.execute("CREATE TABLE s3.newschema.csvtable (id123 bigint, name123 varchar, balance123 double) WITH (FORMAT='CSV', has_header_row = 'false', external_location='s3a://testbucket/TestData/')");
        assertEquals(queryRunner.execute("DESCRIBE s3.newschema.csvtable").getMaterializedRows().size(), 3);
    }

    @Test(dependsOnMethods = "testCreateSchema")
    public void testCreateTableVarcharLimit() {
        log.info("Test: testCreateTable");
        queryRunner.execute("CREATE TABLE s3.newschema.tablevarcharlimit (id123 bigint, name123 varchar(64), balance123 double) WITH (FORMAT='CSV', has_header_row = 'false', external_location='s3a://testbucket/TestDataABC/')");
        boolean foundColumn = false;
        List<MaterializedRow> rows = queryRunner.execute("DESCRIBE s3.newschema.tablevarcharlimit").getMaterializedRows();
        for (MaterializedRow row : rows) {
            log.debug("Test: testCreateTableVarcharLimit.  Found column: " + row.toString());
            if (row.toString().contains("varchar(64)")) {
                foundColumn = true;
            }
        }
        assertTrue(foundColumn);
    }

    @Test(dependsOnMethods = "testCreateTable")
    public void testInsertRow() {
        log.info("Test: testInsertRow");
        queryRunner.execute("INSERT INTO s3.newschema.csvtable VALUES (100, 'JOHN, PAUL, GEORGE, RINGO and \"YOKO\"', 20.0)");
        List<MaterializedRow> rows = queryRunner.execute("SELECT * FROM s3.newschema.csvtable").getMaterializedRows();
        int numRows = 0;
        boolean foundBeatles = false;
        for (MaterializedRow row : rows) {
            log.debug("Test: testInsertRow.  Found row: " + row.toString());
            if (row.toString().contains("JOHN, PAUL, GEORGE, RINGO and \"YOKO\"")) {
                foundBeatles = true;
            }
            numRows++;
        }
        assertTrue(foundBeatles);
        assertTrue(numRows == 1);
    }

    @Test(dependsOnMethods = "testInsertRow")
    public void testCTASInsertRow() {
        log.info("Test: testCTASInsertRow");
        queryRunner.execute("CREATE TABLE s3.newschema.csvtable1 WITH (has_header_row='false', FORMAT='CSV', external_location='s3a://testbucket/TestData1/') as select * from s3.newschema.csvtable");
        assertEquals(queryRunner.execute("SELECT * FROM s3.newschema.csvtable1").getMaterializedRows().size(), 1);
    }

    @Test(dependsOnMethods = "testCTASInsertRow")
    public void testCsvDate() {
        log.info("Test: testCTASInsertRow");

        queryRunner.execute("CREATE TABLE s3.newschema.csvtabledate (id bigint, date_sold date, time_of_date time) WITH (FORMAT='CSV', has_header_row = 'false', external_location='s3a://testbucket/TestDataDate/')");
        assertEquals(queryRunner.execute("DESCRIBE s3.newschema.csvtabledate").getMaterializedRows().size(), 3);

        queryRunner.execute("INSERT INTO s3.newschema.csvtabledate VALUES (27, date '2021-09-20', time '07:00:00.000')");
        MaterializedResult result = queryRunner.execute("SELECT * FROM s3.newschema.csvtabledate");
        assertEquals(result.getRowCount(), 1);

        assertEquals(result.getTypes().get(1), DateType.DATE);
        assertEquals(result.getTypes().get(2), TimeType.TIME);
        assertEquals(result.getMaterializedRows().get(0).getField(1).toString(), "2021-09-20");
        // TODO: check format going in,
        //       insert time above, time '07:00:00.000' gets written to csv as 10:00, and returned here as 23:00
        //assertEquals(result.getMaterializedRows().get(0).getField(2).toString(), "07:00:00.000");
    }

    @Test(dependsOnMethods = "testCsvDate")
    public void testDropTable() {
        log.info("Test: testDropTable");
        queryRunner.execute("DROP TABLE s3.newschema.csvtable");
        queryRunner.execute("DROP TABLE s3.newschema.csvtable1");
        queryRunner.execute("DROP TABLE s3.newschema.csvtabledate");
        List<MaterializedRow> rows = queryRunner.execute("SHOW TABLES in s3.newschema").getMaterializedRows();
        boolean foundTable = false;
        for (MaterializedRow row : rows) {
            log.debug("Test: testDropTable.  Found table: " + row.toString());
            if (row.toString().contains("csvtable")) {
                foundTable = true;
            }
        }
        assertFalse(foundTable);
    }

    @Test(dependsOnMethods = "testCreateSchema")
    public void testCreateTableJson() {
        log.info("Test: testCreateTableJson");
        queryRunner.execute("CREATE TABLE s3.newschema.jsontable (id123 bigint, name123 varchar, balance123 double, date123 date, time123 time) WITH (FORMAT='JSON', has_header_row = 'false', external_location='s3a://testbucket/TestDataJson/')");
        MaterializedResult result = queryRunner.execute("DESCRIBE s3.newschema.jsontable");
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(3).getField(1), "date");
        assertEquals(result.getMaterializedRows().get(4).getField(1), "time");
    }

    @Test(dependsOnMethods = "testCreateTableJson")
    public void testInsertRowJson() {
        log.info("Test: testInsertRowJson");
        queryRunner.execute("INSERT INTO s3.newschema.jsontable VALUES (100, 'GEORGE', 20.0, date '2021-09-20', time '07:00:00.000')");
        assertEquals(queryRunner.execute("SELECT * FROM s3.newschema.jsontable").getMaterializedRows().size(), 1);
    }

    @Test(dependsOnMethods = "testInsertRowJson")
    public void testCTASInsertRowJson() {
        log.info("Test: testCTASInsertRowJson");
        queryRunner.execute("CREATE TABLE s3.newschema.jsontable1 WITH (has_header_row='false', FORMAT='JSON', external_location='s3a://testbucket/TestDataJson1/') as select * from s3.newschema.jsontable");
        assertEquals(queryRunner.execute("SELECT * FROM s3.newschema.jsontable1").getMaterializedRows().size(), 1);
    }

    @Test(dependsOnMethods = "testCTASInsertRowJson")
    public void testDropTableJson() {
        log.info("Test: testDropTableJson");
        queryRunner.execute("DROP TABLE s3.newschema.jsontable");
        queryRunner.execute("DROP TABLE s3.newschema.jsontable1 ");
        List<MaterializedRow> rows = queryRunner.execute("SHOW TABLES in s3.newschema").getMaterializedRows();
        boolean foundTable = false;
        for (MaterializedRow row : rows) {
            log.debug("Test: testDropTableJson.  Found table: " + row.toString());
            if (row.toString().contains("jsontable")) {
                foundTable = true;
            }
        }
        assertFalse(foundTable);
    }

    @Test(dependsOnMethods = "resetSchema",
            expectedExceptions = {RuntimeException.class},
            expectedExceptionsMessageRegExp = "Bucket.*does not exist")
    public void testSchemaConfigBadBucket() {
        log.info("Test: testSchemaConfigBadBucket");
        queryRunner.execute("SELECT * FROM s3.bogusdb.bogusBucketTable");
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testSchemaConfigBadObject() {
        log.info("Test: testSchemaConfigBadObject");
        // bucket exists.  object/prefix could have been listed
        assertEquals(queryRunner.execute("SELECT * FROM s3.bogusdb.bogusObjectTable").getRowCount(), 0);
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testS3BucketsShowTables() {
        log.info("Test: testS3BucketsShowTables");
        assertEquals(queryRunner.execute("SHOW tables in s3.s3_buckets").getRowCount(), 1);
    }

    @Test(dependsOnMethods = "resetSchema",
            expectedExceptions = {RuntimeException.class},
            expectedExceptionsMessageRegExp = "MetaData Search is not Enabled for this Bucket")
    public void getSelectStarFromS3Buckets() {
        log.info("Test: getSelectStarFromS3Buckets");
        queryRunner.execute("select * from s3.s3_buckets.testbucket");
    }

    @Test(dependsOnMethods = "resetSchema")
    public void testJsonDate() {
        MaterializedResult result = queryRunner.execute("select dateCol, timestampCol from s3.types.typesTable where nameCol = 'andrew'");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), DateType.DATE);
        assertEquals(result.getTypes().get(1), TimestampType.TIMESTAMP);
        assertEquals(result.getMaterializedRows().get(0).getField(0).toString(), "2021-05-25");
        assertEquals(result.getMaterializedRows().get(0).getField(1).toString(), "2021-05-25T16:05:15.123");
    }

    static boolean s3SelectEnabledForSession(Session session) {
        for (Map.Entry<String, String> entry : s3Props(session).entrySet()) {
            if (entry.getKey().equals(S3Const.SESSION_PROP_S3_SELECT_PUSHDOWN)) {
                return entry.getValue().equals(S3Const.LC_TRUE);
            }
        }
        return false;
    }

    static Map<String, String> s3Props(Session session) {
        return session.getUnprocessedCatalogProperties().get("s3");
    }

    @AfterSuite
    public void shutdown()
            throws Exception {

        System.out.println("Stop query runners, schema registry and s3 server");
        queryRunner.close();
        queryRunner = null;
        Process p3;

        if (p1 != null) {
            try {
                String[] cmd = {"bash", "src/test/bin/s3_stop.sh"};
                p3 = Runtime.getRuntime().exec(cmd);
                p3.waitFor();
            } catch (Exception e) {
                System.out.println("Exception stopping query runner and s3 server: " + e.toString());
                throw e;
            }
        }

        if (s3Server != null) {
            s3Server.stop();
        }

        if (schemaRegistry != null) {
            schemaRegistry.stop();
            schemaRegistry = null;
        }
    }
}
