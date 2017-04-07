package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IndexScanOperator extends QueryOperator {
  private Database.Transaction transaction;
  private String tableName;
  private String columnName;
  private QueryPlan.PredicateOperator predicate;
  private DataBox value;

  private int columnIndex;

  /**
   * An index scan operator.
   *
   * @param transaction the transaction containing this operator
   * @param tableName the table to iterate over
   * @param columnName the name of the column the index is on
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public IndexScanOperator(Database.Transaction transaction,
                           String tableName,
                           String columnName,
                           QueryPlan.PredicateOperator predicate,
                           DataBox value) throws QueryPlanException, DatabaseException {
    super(OperatorType.INDEXSCAN);
    this.tableName = tableName;
    this.transaction = transaction;
    this.columnName = columnName;
    this.predicate = predicate;
    this.value = value;
    this.setOutputSchema(this.computeSchema());
    columnName = this.checkSchemaForColumn(this.getOutputSchema(), columnName);
    this.columnIndex = this.getOutputSchema().getFieldNames().indexOf(columnName);
  }

  public String toString() {
    return "type: " + this.getType() +
        "\ntable: " + this.tableName +
        "\ncolumn: " + this.columnName +
        "\noperator: " + this.predicate +
        "\nvalue: " + this.value;
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new IndexScanIterator();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class IndexScanIterator implements Iterator<Record> {
    /* TODO: Implement the IndexScanIterator */
    private Record nextRecord;
    private boolean noMoreNext;//flag to show there is no more next any more

    private Iterator<Record> recordIterator;
    public IndexScanIterator() throws QueryPlanException, DatabaseException {
      /* TODO */
      this.recordIterator=null;
      this.nextRecord=null;
      this.noMoreNext=true;

      if(IndexScanOperator.this.tableName==null)
        throw new DatabaseException("No table name");

      if(columnName==null||predicate==null){
        recordIterator=IndexScanOperator.this.transaction.getRecordIterator(IndexScanOperator.this.tableName);
        return;
      }
      //From now, we assume the index exists,
      if(!IndexScanOperator.this.transaction.indexExists(tableName, columnName))
        throw new DatabaseException("No index exists for this column name");

      noMoreNext=false;
      switch(IndexScanOperator.this.predicate) {
        case EQUALS:
          recordIterator = IndexScanOperator.this.transaction.lookupKey(tableName, columnName, value);
          return;
        case GREATER_THAN_EQUALS:
          recordIterator = IndexScanOperator.this.transaction.sortedScanFrom(tableName, columnName, value);
          return;
        case GREATER_THAN:
          recordIterator = IndexScanOperator.this.transaction.sortedScanFrom(tableName, columnName, value);
          return;
        case LESS_THAN:
          recordIterator = IndexScanOperator.this.transaction.sortedScan(tableName, columnName);
          return;
        case LESS_THAN_EQUALS:
          recordIterator = IndexScanOperator.this.transaction.sortedScan(tableName, columnName);
          return;
        case NOT_EQUALS:
          recordIterator = IndexScanOperator.this.transaction.sortedScan(tableName, columnName);
          return;
      }
      throw new DatabaseException("Invalid predicate");

    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      /* TODO */
      if (noMoreNext){
        return false;
      }
      if(this.nextRecord!=null){
        return true;
      }

      if(tableName==null) return false;

      if (columnName==null || IndexScanOperator.this.predicate==null) {
        if(this.recordIterator.hasNext()){
          nextRecord=this.recordIterator.next();
          return true;
        };
        return false;
      }
      switch(predicate){
        case EQUALS:
        case GREATER_THAN_EQUALS:
          if(this.recordIterator.hasNext()){
            nextRecord=this.recordIterator.next();
            return true;
          };
          return false;
      }
      //Now we have to fetch the record to check if condition is satisfied
      while(this.recordIterator.hasNext()) {
        this.nextRecord=this.recordIterator.next();
        DataBox colValue = this.nextRecord.getValues().get(columnIndex);
        int cp= colValue.compareTo(IndexScanOperator.this.value);
        switch (predicate) {
          case GREATER_THAN:
            if(cp>0)
              return true;
            break;
          case LESS_THAN:
            if(cp<0)
              return true;
            else {
              noMoreNext=true;
              return false;//this is no point to loop anymore as rest of records is in a sorted order
            }
          case LESS_THAN_EQUALS:
            if(cp<=0)
              return true;
            else {
              noMoreNext=true;
              return false;//this is no point to loop anymore as rest of records is in a sorted order
            }
          case NOT_EQUALS:
            if(cp!=0)
              return true;
        }
      }
      return false;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      /* TODO */
      if(this.hasNext() ){
        Record r = this.nextRecord;
        this.nextRecord = null;
        return r;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
