package edu.berkeley.cs186.database.query;

import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP4;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.table.MarkerRecord;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.StringHistogram;

import static org.junit.Assert.*;

public class TestOptimalQueryPlanJoins {
  private Database database;
  private Random random = new Random();
  private String alphabet = StringHistogram.alphaNumeric;
  private String defaulTableName = "testAllTypes";
  private String testTable1Name = "testAllTypesIndex";
  private int defaultNumRecords = 1000;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("db");
    this.database = new Database(tempDir.getAbsolutePath());
    this.database.deleteAllTables();
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), this.defaulTableName);
    Database.Transaction transaction = this.database.beginTransaction();

    // by default, create 100 records
    for (int i = 0; i < this.defaultNumRecords; i++) {
      // generate a random record
      IntDataBox intValue = new IntDataBox(i);
      FloatDataBox floatValue = new FloatDataBox(this.random.nextFloat());
      BoolDataBox boolValue = new BoolDataBox(this.random.nextBoolean());
      String stringValue = "";

      for (int j = 0 ; j < 5; j++) {
        int randomIndex = Math.abs(this.random.nextInt() % alphabet.length());
        stringValue += alphabet.substring(randomIndex, randomIndex + 1);
      }

      List<DataBox> values = new ArrayList<DataBox>();
      values.add(boolValue);
      values.add(intValue);
      values.add(new StringDataBox(stringValue, 5));
      values.add(floatValue);

      transaction.addRecord("testAllTypes", values);
    }
    setUpIndex(this.database, transaction);
    transaction.end();
  }

  /*
  Create another table with "int" column indexed
   */
  public void setUpIndex(Database d, Database.Transaction transaction)
          throws DatabaseException, IOException {

    Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
    List<DataBox> r1Vals = r1.getValues();
    Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
    List<DataBox> r2Vals = r2.getValues();
    Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
    List<DataBox> r3Vals = r3.getValues();
    Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
    List<DataBox> r4Vals = r4.getValues();
    Record r5 = TestUtils.createRecordWithAllTypesWithValue(5);
    List<DataBox> r5Vals = r5.getValues();
    Record r6 = TestUtils.createRecordWithAllTypesWithValue(6);
    List<DataBox> r6Vals = r6.getValues();
    List<String> indexList = new ArrayList<String>();

    indexList.add("int");
    d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(),this.testTable1Name, indexList);
    for (int i = 0; i < 1000; i++) {//a page contains 288 records. We build a big table to show the advantage of indexing
      transaction.addRecord(this.testTable1Name, r5Vals);
      transaction.addRecord(this.testTable1Name, r2Vals);
      transaction.addRecord(this.testTable1Name, r1Vals);
      transaction.addRecord(this.testTable1Name, r6Vals);
    }
    for (int i = 0; i < 2; i++) {//a page contains 288 records. We build a big table to show the advantage of indexing
    	transaction.addRecord(this.testTable1Name, r3Vals);
    }
  }    


  //@Test(timeout=5000)
  @Test
  public void testSimpleJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");
    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.int", "t2.int");
    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;

    while (outputIterator.hasNext()) {
      count++;

      Record record = outputIterator.next();
      List<DataBox> recordValues = record.getValues();
      assertEquals(recordValues.get(0), recordValues.get(4));
      assertEquals(recordValues.get(1), recordValues.get(5));
      assertEquals(recordValues.get(2), recordValues.get(6));
      assertEquals(recordValues.get(3), recordValues.get(7));
    }

    assertTrue(count == this.defaultNumRecords);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: BNLJ\n" +
                  "leftColumn: t2.int\n" +
                  "rightColumn: t1.int\n" +
                  "\t(left)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t1";
    String tree2 = "type: BNLJ\n" +
                  "leftColumn: t1.int\n" +
                  "rightColumn: t2.int\n" +
                  "\t(left)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t1\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2";
    assertTrue(finalOperator.toString().equals(tree) || finalOperator.toString().equals(tree2));

    transaction.end();
  }
  //@Test(timeout=5000)
  @Test
  @Category(StudentTestP4.class)
  public void testSimpleJoinIterator1() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");
    transaction.queryAs(this.defaulTableName, "t3");
    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.int", "t2.int");
    queryPlan.join("t3", "t1.int", "t3.int");
    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;

    while (outputIterator.hasNext()) {
      count++;

      Record record = outputIterator.next();
      List<DataBox> recordValues = record.getValues();
      assertEquals(recordValues.get(0), recordValues.get(4));
      assertEquals(recordValues.get(0), recordValues.get(8));
      assertEquals(recordValues.get(1), recordValues.get(5));
      assertEquals(recordValues.get(1), recordValues.get(9));
      assertEquals(recordValues.get(2), recordValues.get(6));
      assertEquals(recordValues.get(2), recordValues.get(10));
      assertEquals(recordValues.get(3), recordValues.get(7));
      assertEquals(recordValues.get(3), recordValues.get(11));
    }

    assertTrue(count == this.defaultNumRecords);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String myTree="type: BNLJ\n" +
            "leftColumn: t1.int\n" +
            "rightColumn: t2.int\n" +
            "\t(left)\n" +
            "\ttype: BNLJ\n" +
            "\tleftColumn: t3.int\n" +
            "\trightColumn: t1.int\n" +
            "\t\t(left)\n" +
            "\t\ttype: SEQSCAN\n" +
            "\t\ttable: t3\n" +
            "\t\n" +
            "\t\t(right)\n" +
            "\t\ttype: SEQSCAN\n" +
            "\t\ttable: t1\n" +
            "\n" +
            "\t(right)\n" +
            "\ttype: SEQSCAN\n" +
            "\ttable: t2";
    String finalTree=finalOperator.toString();
    //assertTrue(finalOperator.toString().equals(tree) || finalOperator.toString().equals(tree2));
    assertTrue(myTree.equals(finalTree));
    transaction.end();
  }

  //@Test
  @Test(timeout=5000)
  public void testProjectJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.int");
    columnNames.add("t2.string");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    int count = 0;
    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof IntDataBox);
      assertTrue(values.get(1) instanceof StringDataBox);

      count++;
    }

    // We test `>=` instead of `==` since strings are generated
    // randomly and there's a small chance of duplicates.
    assertTrue(count >= 1000);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: PROJECT\n" +
                  "columns: [t1.int, t2.string]\n" +
                  "\ttype: BNLJ\n" +
                  "\tleftColumn: t2.string\n" +
                  "\trightColumn: t1.string\n" +
                  "\t\t(left)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t2\n" +
                  "\t\n" +
                  "\t\t(right)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t1";
    String tree2 = "type: PROJECT\n" +
                  "columns: [t1.int, t2.string]\n" +
                  "\ttype: BNLJ\n" +
                  "\tleftColumn: t1.string\n" +
                  "\trightColumn: t2.string\n" +
                  "\t\t(left)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t1\n" +
                  "\t\n" +
                  "\t\t(right)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t2";
    assertTrue(finalOperator.toString().equals(tree) || finalOperator.toString().equals(tree2));

    transaction.end();
  }

  @Test(timeout=5000)
 // @Test
  public void testSelectJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(values.get(0), values.get(4));
      assertEquals(values.get(1), values.get(5));
      assertEquals(values.get(2), values.get(6));
      assertEquals(values.get(3), values.get(7));

      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: BNLJ\n" +
                  "leftColumn: t1.string\n" +
                  "rightColumn: t2.string\n" +
                  "\t(left)\n" +
                  "\ttype: SELECT\n" +
                  "\tcolumn: t1.bool\n" +
                  "\toperator: NOT_EQUALS\n" +
                  "\tvalue: false\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t1\n" +
                  "\n" +
                  "\t(right)\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: t2";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }
  //@Test(timeout=5000)
 @Test
  public void testSelectJoinIterator1() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");
    transaction.queryAs(this.defaulTableName, "t3");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.join("t3", "t1.string", "t3.string");
    queryPlan.select("t2.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));
    queryPlan.select("t3.bool", QueryPlan.PredicateOperator.EQUALS, new BoolDataBox(true));
    queryPlan.select("t1.int", QueryPlan.PredicateOperator.LESS_THAN, new IntDataBox(50));

   Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(values.get(0), values.get(4));
      assertEquals(values.get(0), values.get(8));
      assertEquals(values.get(1), values.get(5));
      assertEquals(values.get(1), values.get(9));
      assertEquals(values.get(2), values.get(6));
      assertEquals(values.get(2), values.get(10));
      assertEquals(values.get(3), values.get(7));
      assertEquals(values.get(3), values.get(11));

      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "";
    String finalTree=finalOperator.toString();
    //assertEquals(tree, finalTree);
    assertEquals(QueryOperator.OperatorType.JOIN, finalOperator.getType())  ;
    transaction.end();
  }

  @Test(timeout=5000)
  public void testProjectSelectJoinIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.bool");
    columnNames.add("t2.int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);

      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: PROJECT\n" +
                  "columns: [t1.bool, t2.int]\n" +
                  "\ttype: BNLJ\n" +
                  "\tleftColumn: t1.string\n" +
                  "\trightColumn: t2.string\n" +
                  "\t\t(left)\n" +
                  "\t\ttype: SELECT\n" +
                  "\t\tcolumn: t1.bool\n" +
                  "\t\toperator: NOT_EQUALS\n" +
                  "\t\tvalue: false\n" +
                  "\t\t\ttype: SEQSCAN\n" +
                  "\t\t\ttable: t1\n" +
                  "\t\n" +
                  "\t\t(right)\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: t2";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }
  //@Test(timeout=5000)
  @Test
  @Category(StudentTestP4.class)
  public void testProjectSelectJoinIterator2() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));
    queryPlan.select("t2.int", QueryPlan.PredicateOperator.EQUALS, new IntDataBox(2));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.bool");
    columnNames.add("t2.int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);
      assertEquals(new IntDataBox(2), values.get(1));
      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();

    //assertEquals(tree, finalOperator.toString());
    String tree= "type: PROJECT\n" +
            "columns: [t1.bool, t2.int]\n" +
            "\ttype: SNLJ\n" +
            "\tleftColumn: t2.string\n" +
            "\trightColumn: t1.string\n" +
            "\t\t(left)\n" +
            "\t\ttype: SELECT\n" +
            "\t\tcolumn: t2.int\n" +
            "\t\toperator: EQUALS\n" +
            "\t\tvalue: 2\n" +
            "\t\t\ttype: SEQSCAN\n" +
            "\t\t\ttable: t2\n" +
            "\t\n" +
            "\t\t(right)\n" +
            "\t\ttype: SELECT\n" +
            "\t\tcolumn: t1.bool\n" +
            "\t\toperator: NOT_EQUALS\n" +
            "\t\tvalue: false\n" +
            "\t\t\ttype: SEQSCAN\n" +
            "\t\t\ttable: t1";
    String finalTree=finalOperator.toString();
    assertEquals(tree, finalTree);

    transaction.end();
  }
  //@Test(timeout=5000)
  @Test
  @Category(StudentTestP4.class)
  public void testProjectSelectJoinIterator3() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));
    queryPlan.select("t2.int", QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(49));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.bool");
    columnNames.add("t2.int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);
      assertTrue(values.get(1).getInt() >49);
      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();

    //assertEquals(tree, finalOperator.toString());

    String tree="type: PROJECT\n" +
            "columns: [t1.bool, t2.int]\n" +
            "\ttype: BNLJ\n" +
            "\tleftColumn: t1.string\n" +
            "\trightColumn: t2.string\n" +
            "\t\t(left)\n" +
            "\t\ttype: SELECT\n" +
            "\t\tcolumn: t1.bool\n" +
            "\t\toperator: NOT_EQUALS\n" +
            "\t\tvalue: false\n" +
            "\t\t\ttype: SEQSCAN\n" +
            "\t\t\ttable: t1\n" +
            "\t\n" +
            "\t\t(right)\n" +
            "\t\ttype: SELECT\n" +
            "\t\tcolumn: t2.int\n" +
            "\t\toperator: GREATER_THAN\n" +
            "\t\tvalue: 49\n" +
            "\t\t\ttype: SEQSCAN\n" +
            "\t\t\ttable: t2";
    String finalTree=finalOperator.toString();
    assertEquals(tree, finalTree);

    transaction.end();
  }
  //@Test(timeout=5000)
  @Test
  @Category(StudentTestP4.class)
  public void testProjectSelectJoinIterator4() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));
    queryPlan.select("t2.int", QueryPlan.PredicateOperator.LESS_THAN_EQUALS, new IntDataBox(49));
    queryPlan.select("t1.int", QueryPlan.PredicateOperator.LESS_THAN, new IntDataBox(50));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.bool");
    columnNames.add("t2.int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);
      assertTrue(values.get(1).getInt() <50);
      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();

    //assertEquals(tree, finalOperator.toString());

    String tree="type: PROJECT\n" +
            "columns: [t1.bool, t2.int]\n" +
            "\ttype: PNLJ\n" +
            "\tleftColumn: t2.string\n" +
            "\trightColumn: t1.string\n" +
            "\t\t(left)\n" +
            "\t\ttype: SELECT\n" +
            "\t\tcolumn: t2.int\n" +
            "\t\toperator: LESS_THAN_EQUALS\n" +
            "\t\tvalue: 49\n" +
            "\t\t\ttype: SEQSCAN\n" +
            "\t\t\ttable: t2\n" +
            "\t\n" +
            "\t\t(right)\n" +
            "\t\ttype: SELECT\n" +
            "\t\tcolumn: t1.int\n" +
            "\t\toperator: LESS_THAN\n" +
            "\t\tvalue: 50\n" +
            "\t\t\ttype: SELECT\n" +
            "\t\t\tcolumn: t1.bool\n" +
            "\t\t\toperator: NOT_EQUALS\n" +
            "\t\t\tvalue: false\n" +
            "\t\t\t\ttype: SEQSCAN\n" +
            "\t\t\t\ttable: t1";

    String finalTree=finalOperator.toString();
    assertEquals(tree, finalTree);

    transaction.end();
  }

  //@Test(timeout=5000)
  @Test
  @Category(StudentTestP4.class)
  public void testProjectSelectJoinIterator4b() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.testTable1Name, "t2"); //use a table with column "int" indexed

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));
    queryPlan.select("t2.int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(4));
    queryPlan.select("t1.int", QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(3));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.bool");
    columnNames.add("t2.int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);
      assertTrue(values.get(1).getInt() >3);
      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();

    //assertEquals(tree, finalOperator.toString());

    String tree=   "";


    String finalTree=finalOperator.toString();
    //assertEquals(tree, finalTree);

    transaction.end();
  }

  //@Test(timeout=5000)
  @Test
  @Category(StudentTestP4.class)
  public void testProjectSelectJoinIterator4c() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.testTable1Name, "t2"); //use a table with column "int" indexed

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));
    queryPlan.select("t2.int", QueryPlan.PredicateOperator.EQUALS, new IntDataBox(3));
    queryPlan.select("t1.int", QueryPlan.PredicateOperator.EQUALS, new IntDataBox(3));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.bool");
    columnNames.add("t2.int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);
      assertTrue(values.get(1).getInt() ==3);
      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();

    //assertEquals(tree, finalOperator.toString());

    String tree=   "";


    String finalTree=finalOperator.toString();
    //assertEquals(tree, finalTree);

    transaction.end();
  }
  //@Test(timeout=5000)
  @Test
  @Category(StudentTestP4.class)
  public void testProjectSelectJoinIterator5() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    transaction.queryAs(this.defaulTableName, "t1");
    transaction.queryAs(this.defaulTableName, "t2");
    transaction.queryAs(this.defaulTableName, "t3");

    QueryPlan queryPlan = transaction.query("t1");

    queryPlan.join("t2", "t1.string", "t2.string");
    queryPlan.join("t3", "t1.string", "t3.string");
    queryPlan.select("t1.bool", QueryPlan.PredicateOperator.NOT_EQUALS, new BoolDataBox(false));
    queryPlan.select("t2.int", QueryPlan.PredicateOperator.LESS_THAN_EQUALS, new IntDataBox(49));
    queryPlan.select("t1.int", QueryPlan.PredicateOperator.LESS_THAN, new IntDataBox(50));
    queryPlan.select("t3.int", QueryPlan.PredicateOperator.LESS_THAN, new IntDataBox(50));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("t1.bool");
    columnNames.add("t2.int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());

      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);
      assertTrue(values.get(1).getInt() <50);
      assertTrue(values.get(0).getBool());
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();

    //assertEquals(tree, finalOperator.toString());

    String tree="type: PROJECT\n" +
            "columns: [t1.bool, t2.int]\n" +
            "\ttype: PNLJ\n" +
            "\tleftColumn: t1.string\n" +
            "\trightColumn: t2.string\n" +
            "\t\t(left)\n" +
            "\t\ttype: PNLJ\n" +
            "\t\tleftColumn: t3.string\n" +
            "\t\trightColumn: t1.string\n" +
            "\t\t\t(left)\n" +
            "\t\t\ttype: SELECT\n" +
            "\t\t\tcolumn: t3.int\n" +
            "\t\t\toperator: LESS_THAN\n" +
            "\t\t\tvalue: 50\n" +
            "\t\t\t\ttype: SEQSCAN\n" +
            "\t\t\t\ttable: t3\n" +
            "\t\t\n" +
            "\t\t\t(right)\n" +
            "\t\t\ttype: SELECT\n" +
            "\t\t\tcolumn: t1.int\n" +
            "\t\t\toperator: LESS_THAN\n" +
            "\t\t\tvalue: 50\n" +
            "\t\t\t\ttype: SELECT\n" +
            "\t\t\t\tcolumn: t1.bool\n" +
            "\t\t\t\toperator: NOT_EQUALS\n" +
            "\t\t\t\tvalue: false\n" +
            "\t\t\t\t\ttype: SEQSCAN\n" +
            "\t\t\t\t\ttable: t1\n" +
            "\t\n" +
            "\t\t(right)\n" +
            "\t\ttype: SELECT\n" +
            "\t\tcolumn: t2.int\n" +
            "\t\toperator: LESS_THAN_EQUALS\n" +
            "\t\tvalue: 49\n" +
            "\t\t\ttype: SEQSCAN\n" +
            "\t\t\ttable: t2";

    String finalTree=finalOperator.toString();
    assertEquals(tree, finalTree);

    transaction.end();
  }

}
