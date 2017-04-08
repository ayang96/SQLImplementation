package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.io.Page;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP3;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.rules.TemporaryFolder;


import javax.management.Query;

import static org.junit.Assert.*;

public class TestIndexScanOperator {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test(timeout=5000)
    public void testIndexScanEqualsRecords() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

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
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord3 = new Record(expectedRecordValues3);
        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);

        for (int i = 0; i < 99; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int", QueryPlan.PredicateOperator.EQUALS, new IntDataBox(3));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;

        while (outputIterator.hasNext()) {
            if (count < 99) {
                assertEquals(expectedRecord3, outputIterator.next());
            }
            count++;
        }
        assertTrue(count == 99);
    }


    @Test
    @Category(StudentTestP3.class)
    public void testIndexScanPredRecords() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

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
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord3 = new Record(expectedRecordValues3);
        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);
        int total=10;
        for (int i = 0; i < total; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(3));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;
        int expected_count=2*total;

        while (outputIterator.hasNext()) {
            Record r= outputIterator.next();
            count++;
        }
        assertTrue(count == expected_count);

        s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(3));
        outputIterator = s1.iterator();
        count = 0;
        expected_count=3*total;

        while (outputIterator.hasNext()) {
            Record r= outputIterator.next();
            count++;
        }
        assertTrue(count == expected_count);

        s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.LESS_THAN_EQUALS, new IntDataBox(3));
        outputIterator = s1.iterator();
        count = 0;
        expected_count=3*total;

        while (outputIterator.hasNext()) {
            Record r= outputIterator.next();
            count++;
        }
        assertTrue(count == expected_count);

        s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.LESS_THAN, new IntDataBox(3));
        outputIterator = s1.iterator();
        count = 0;
        expected_count=2*total;
        DataBox val=new IntDataBox(3);
        while (outputIterator.hasNext()) {
            Record r= outputIterator.next();
            DataBox colValue = r.getValues().get(1);
            int cp= colValue.compareTo(val);
            assertTrue(cp<0);
            count++;
        }
        assertTrue(count == expected_count);

        s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.NOT_EQUALS, new IntDataBox(3));
        outputIterator = s1.iterator();
        count = 0;
        expected_count=4*total;

        while (outputIterator.hasNext()) {
            Record r= outputIterator.next();
            count++;
        }
        assertTrue(count == expected_count);
    }
    @Test
    @Category(StudentTestP3.class)
    public void testIndexScanPredRecordsFrom1() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

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
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord3 = new Record(expectedRecordValues3);
        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);
        int total=10;
        for (int i = 0; i < total; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(1));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;
        int expected_count=4*total;

        while (outputIterator.hasNext()) {
            Record r= outputIterator.next();
            count++;
        }
        assertTrue(count == expected_count);

        s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(1));
        outputIterator = s1.iterator();
        count = 0;
        expected_count=5*total;

        while (outputIterator.hasNext()) {
            Record r= outputIterator.next();
            count++;
        }
        assertTrue(count == expected_count);

        s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.LESS_THAN_EQUALS, new IntDataBox(1));
        outputIterator = s1.iterator();
        count = 0;
        expected_count=1*total;

        while (outputIterator.hasNext()) {
            Record r= outputIterator.next();
            count++;
        }
        assertTrue(count == expected_count);

        s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.LESS_THAN, new IntDataBox(1));
        outputIterator = s1.iterator();
        count = 0;
        expected_count=0*total;
        DataBox val=new IntDataBox(3);
        while (outputIterator.hasNext()) {
            Record r= outputIterator.next();
            DataBox colValue = r.getValues().get(1);
            int cp= colValue.compareTo(val);
            assertTrue(cp<0);
            count++;
        }
        assertTrue(count == expected_count);

        s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.NOT_EQUALS, new IntDataBox(1));
        outputIterator = s1.iterator();
        count = 0;
        expected_count=4*total;

        while (outputIterator.hasNext()) {
            Record r= outputIterator.next();
            count++;
        }
        assertTrue(count == expected_count);
    }
    @Test
    @Category(StudentTestP3.class)
    public void testIndexScanGreaterThan() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

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
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }

        Record expectedRecord5 = new Record(expectedRecordValues5);
        Record expectedRecord6 = new Record(expectedRecordValues6);
        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);
        int total=10;
        for (int i = 0; i < total; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.GREATER_THAN, new IntDataBox(3));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;
        int expected_count=2*total;

        while (outputIterator.hasNext()) {

            Record r= outputIterator.next();
            assertTrue(r.equals(expectedRecord5)||r.equals(expectedRecord6));
            count++;
        }
        assertTrue(count == expected_count);

    }

    @Test
    @Category(StudentTestP3.class)
    public void testIndexScanGreaterThanEqual() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

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
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }
        Record expectedRecord1 = new Record(expectedRecordValues1);
        Record expectedRecord2 = new Record(expectedRecordValues2);
        Record expectedRecord3 = new Record(expectedRecordValues3);
        Record expectedRecord5 = new Record(expectedRecordValues5);
        Record expectedRecord6 = new Record(expectedRecordValues6);
        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);
        int total=10;
        for (int i = 0; i < total; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(3));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;
        int expected_count=3*total;

        while (outputIterator.hasNext()) {

            Record r= outputIterator.next();
            assertTrue(r.equals(expectedRecord5)||r.equals(expectedRecord6)||r.equals(expectedRecord3));
            count++;
        }
        assertTrue(count == expected_count);

    }
    @Test
    @Category(StudentTestP3.class)
    public void testIndexScanLessThan() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

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
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }
        Record expectedRecord1 = new Record(expectedRecordValues1);
        Record expectedRecord2 = new Record(expectedRecordValues2);
        Record expectedRecord3 = new Record(expectedRecordValues3);
        Record expectedRecord5 = new Record(expectedRecordValues5);
        Record expectedRecord6 = new Record(expectedRecordValues6);
        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);
        int total=10;
        for (int i = 0; i < total; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.LESS_THAN, new IntDataBox(3));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;
        int expected_count=2*total;

        while (outputIterator.hasNext()) {

            Record r= outputIterator.next();
            assertTrue(r.equals(expectedRecord1)||r.equals(expectedRecord2));
            count++;
        }
        assertTrue(count == expected_count);

    }

    @Test(timeout=5000)
    @Category(StudentTestP3.class)
    public void testSimpleSortMergeOutputOrder1() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath());
        Database.Transaction transaction = d.beginTransaction();
        Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
        List<DataBox> r1Vals = r1.getValues();
        Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
        List<DataBox> r2Vals = r2.getValues();

        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        for (int i = 0; i < 2; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
        }

        Record expectedRecord1 = new Record(expectedRecordValues1);
        Record expectedRecord2 = new Record(expectedRecordValues2);
        d.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
        d.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

        int total=144; //Even number only. leftTable contains 2*total of the same records and
        // each page can hold upto 288 or 289 records
        // If total >=146, records of the same key spreads to two different pages
        //The  specification says no backtracking is needed and
        // we can assume records of the same key are in the same page. Thus set total=144 or less
        for (int i = 0; i < total; i++) {
            List<DataBox> vals;
            if (i < total/2) {
                vals = r1Vals;
            } else {
                vals = r2Vals;
            }
            transaction.addRecord("leftTable", vals);
            transaction.addRecord("rightTable", vals);
        }


        QueryOperator s1 = new SequentialScanOperator(transaction,"leftTable");
        QueryOperator s2 = new SequentialScanOperator(transaction,"rightTable");
        QueryOperator joinOperator = new SortMergeOperator(s1, s2, "int", "int", transaction);

        int count = 0;
        Iterator<Record> outputIterator = joinOperator.iterator();

        while (outputIterator.hasNext()) {
            if (count < total*total/4) {
                assertEquals(expectedRecord1, outputIterator.next());
            } else  {
                assertEquals(expectedRecord2, outputIterator.next());
            }
            count++;
        }

        assertTrue(count == total*total/2);
    }
    @Test
    @Category(StudentTestP3.class)
    public void testIndexScanLessThanEquals() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

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
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }
        Record expectedRecord1 = new Record(expectedRecordValues1);
        Record expectedRecord2 = new Record(expectedRecordValues2);
        Record expectedRecord3 = new Record(expectedRecordValues3);
        Record expectedRecord5 = new Record(expectedRecordValues5);
        Record expectedRecord6 = new Record(expectedRecordValues6);
        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);
        int total=10;
        for (int i = 0; i < total; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.LESS_THAN_EQUALS, new IntDataBox(3));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;
        int expected_count=3*total;

        while (outputIterator.hasNext()) {

            Record r= outputIterator.next();
            assertTrue(r.equals(expectedRecord1)||r.equals(expectedRecord2)||r.equals(expectedRecord3));
            count++;
        }
        assertTrue(count == expected_count);

    }

    @Test
    @Category(StudentTestP3.class)
    public void testIndexScanNotEquals() throws QueryPlanException, DatabaseException, IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        Database d = new Database(tempDir.getAbsolutePath(), 4);
        Database.Transaction transaction = d.beginTransaction();

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
        List<DataBox> expectedRecordValues1 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues2 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues3 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues4 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues5 = new ArrayList<DataBox>();
        List<DataBox> expectedRecordValues6 = new ArrayList<DataBox>();


        for (int i = 0; i < 1; i++) {
            for (DataBox val: r1Vals) {
                expectedRecordValues1.add(val);
            }
            for (DataBox val: r2Vals) {
                expectedRecordValues2.add(val);
            }
            for (DataBox val: r3Vals) {
                expectedRecordValues3.add(val);
            }
            for (DataBox val: r4Vals) {
                expectedRecordValues4.add(val);
            }
            for (DataBox val: r5Vals) {
                expectedRecordValues5.add(val);
            }
            for (DataBox val: r6Vals) {
                expectedRecordValues6.add(val);
            }
        }
        Record expectedRecord1 = new Record(expectedRecordValues1);
        Record expectedRecord2 = new Record(expectedRecordValues2);
        Record expectedRecord3 = new Record(expectedRecordValues3);
        Record expectedRecord5 = new Record(expectedRecordValues5);
        Record expectedRecord6 = new Record(expectedRecordValues6);
        List<String> indexList = new ArrayList<String>();
        indexList.add("int");
        d.createTableWithIndices(TestUtils.createSchemaWithAllTypes(), "myTable", indexList);
        int total=10;
        for (int i = 0; i < total; i++) {
            transaction.addRecord("myTable", r3Vals);
            transaction.addRecord("myTable", r5Vals);
            transaction.addRecord("myTable", r2Vals);
            transaction.addRecord("myTable", r1Vals);
            transaction.addRecord("myTable", r6Vals);
        }

        QueryOperator s1 = new IndexScanOperator(transaction,"myTable", "int",
                QueryPlan.PredicateOperator.NOT_EQUALS, new IntDataBox(3));
        Iterator<Record> outputIterator = s1.iterator();
        int count = 0;
        int expected_count=4*total;

        while (outputIterator.hasNext()) {

            Record r= outputIterator.next();
            assertTrue(!r.equals(expectedRecord3));
            count++;
        }
        assertTrue(count == expected_count);

    }
}