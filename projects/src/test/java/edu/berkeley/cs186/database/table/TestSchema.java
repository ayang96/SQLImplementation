package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestSchema {
  @Test
  public void testSchemaRetrieve() {
    Schema schema = TestUtils.createSchemaWithAllTypes();

    Record input = TestUtils.createRecordWithAllTypes();
    byte[] encoded = schema.encode(input);
    Record decoded = schema.decode(encoded);

    assertEquals(input, decoded);
  }

  @Test
  public void testValidRecord() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    try {
      Record output = schema.verify(input.getValues());
      assertEquals(input, output);
    } catch (SchemaException se) {
      fail();
    }
  }

  @Test(expected = SchemaException.class)
  public void testInvalidRecordLength() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    schema.verify(new ArrayList<DataBox>());
  }

  @Test(expected = SchemaException.class)
  public void testInvalidFields() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataBox> values = new ArrayList<DataBox>();

    values.add(new StringDataBox("abcde", 5));
    values.add(new IntDataBox(10));

    schema.verify(values);
  }
  @Test
  @Category(StudentTest.class)
  public void testNewSchema() {
    List<String> fields = new ArrayList<String>();
    List<DataBox> fieldTypes = new ArrayList<DataBox>();
    List<DataBox> values = new ArrayList<DataBox>();
    fields.add("Name");
    fields.add("Major");
    fields.add("Age");
    fieldTypes.add(new StringDataBox(10));
    fieldTypes.add(new StringDataBox(10));
    fieldTypes.add(new IntDataBox());
    Schema studentSchema = new Schema(fields,fieldTypes);
    values.add(new StringDataBox("Alex",10));
    values.add(new StringDataBox("Comp Sci",10));
    values.add(new IntDataBox(20));
    try {
      studentSchema.verify(values);
    } catch (SchemaException se) {
      fail();
    }
  }
  @Test(expected = SchemaException.class)
  @Category(StudentTest.class)
  public void testFalseStringLength() throws SchemaException{
    List<String> fields = new ArrayList<String>();
    List<DataBox> fieldTypes = new ArrayList<DataBox>();
    List<DataBox> values = new ArrayList<DataBox>();
    fields.add("Name");
    fields.add("Major");
    fields.add("Age");
    fieldTypes.add(new StringDataBox(10));
    fieldTypes.add(new StringDataBox(10));
    fieldTypes.add(new IntDataBox());
    Schema studentSchema = new Schema(fields,fieldTypes);
    values.add(new StringDataBox("Alex",9));
    values.add(new StringDataBox("Comp Sciasfasefaefasefasefasfasfasdfsafsa",11));
    values.add(new IntDataBox(20));
    studentSchema.verify(values);
  }
}