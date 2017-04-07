package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator implements Iterator<Record> {
    /* TODO: Implement the PNLJIterator */
    /* Suggested Fields */
    private String leftTableName;
    private String rightTableName;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;
    private Page rightPage;
    private byte[] leftHeader;
    private byte[] rightHeader;
    private int leftEntryNum;
    private int rightEntryNum;
    // add these fields tempororally
    private int leftNo_of_Entries, rightNo_of_Entries;
    private Schema leftSchema,rightSchema;
    private int leftEntrySize, rightEntrySize, leftHeaderSize, rightHeaderSize;


    public PNLJIterator() throws QueryPlanException, DatabaseException {
      /* Suggested Starter Code: get table names. */
      if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
        PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (PNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
        PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      /* TODO */
      //Initialize the iterator attributes. Notice PNLJOperator.this is a JoinOperator.
      this.leftIterator=PNLJOperator.this.getPageIterator(this.leftTableName);
      this.leftIterator.next();//skip page 0
      this.rightIterator=PNLJOperator.this.getPageIterator(this.rightTableName);
      this.rightIterator.next();//skip page 0
      leftRecord=nextRecord=rightRecord=null;
      leftPage= rightPage=null;
      leftNo_of_Entries=PNLJOperator.this.getNumEntriesPerPage(this.leftTableName);
      rightNo_of_Entries=PNLJOperator.this.getNumEntriesPerPage(this.rightTableName);
      leftHeader= rightHeader=null;
      leftEntryNum= rightEntryNum=0;//to be fetched next
      leftSchema=PNLJOperator.this.getLeftSource().getOutputSchema();
      rightSchema=PNLJOperator.this.getRightSource().getOutputSchema();
      leftHeaderSize=PNLJOperator.this.getHeaderSize(this.leftTableName);
      rightHeaderSize=PNLJOperator.this.getHeaderSize(this.rightTableName);
      leftEntrySize=PNLJOperator.this.getEntrySize(this.leftTableName);
      rightEntrySize=PNLJOperator.this.getEntrySize(this.rightTableName);
      this.getNextLeftPage();
      this.getNextRightPage();

    }
    /*
      Read a record from a page
      @p  Page ID
      @s  Record schema
      @entryNum, then entry number
      @limit the total number of entry number per page
      @currHeader the header of this page
      @entrySize,  the size of each entry (record).
      @pageHeaderSize,  the size of header
     */
    private Record getRecord(Page p, Schema s, int entryNum, int limit, byte currHeader[], int entrySize, int pageHeaderSize) {
      if (entryNum < limit) {
        byte b = currHeader[entryNum / 8];
        int bitOffset = 7 - (entryNum % 8);
        byte mask = (byte) (1 << bitOffset);

        byte value = (byte) (b & mask);
        if (value != 0) {
          int offset = pageHeaderSize + (entrySize * entryNum);
          byte[] bytes = p.readBytes(offset, entrySize);

          Record toRtn = s.decode(bytes);
          return toRtn;
        }
      }
      return null;
    }

    public boolean hasNext() {
      /* TODO */
      if (this.nextRecord != null) {
        return true;
      }
      while (true) {
        while(this.leftRecord == null) {
          if (this.leftEntryNum >= this.leftNo_of_Entries) { //we have scanned all records of this page
            //we should set to read to the next page of right source
            if(this.rightIterator.hasNext()) {
              this.leftEntryNum = 0;
              if( !getNextRightPage())
                return false;
            } else {//nothing to scan from the right source, then we have to
                    // increment pointer to the left source page
              if (this.leftIterator.hasNext()) {
                if( !getNextLeftPage())
                  return false;
                try { //We start from page 0 for the right source
                  this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);
                } catch (DatabaseException e) {
                  return false;
                }
                if( !getNextRightPage())
                  return false;

              } else //there is nothing to read any more.
                return false;
            }
          }  //Read left record
          this.leftRecord = getNextLeftRecordInPage();
          this.leftEntryNum++;
          this.rightEntryNum=0;//We restart from entry 0 of the right source page
          //It is still possible that leftRecord=null as we may read a null record
        }
        //Now leftRecord!=null.

        if (rightRecord == null) {//Need to get right next record
          if (this.rightEntryNum < this.rightNo_of_Entries ) {
            this.rightRecord = getNextRightRecordInPage();
            this.rightEntryNum++;
          } else { //there is no more record to process in this right source page.
            this.leftRecord = null;//We have to move on the left pointer.
          }
        }

        if (this.rightRecord != null) {
          DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = this.rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<DataBox>(this.rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
            this.rightRecord=null; //We have used this right one, move to next one.
            return true;
          }
          this.rightRecord = null; //We have used this record right, thus need to move on
        }

      }
    }
    /*
     Read next page from the right source
     @return: true means successful. false means failure.
     */
    private boolean getNextRightPage() {
      /* TODO */
      this.rightPage = this.rightIterator.next();
      try {
        rightHeader = PNLJOperator.this.getPageHeader(this.rightTableName, rightPage);
      } catch (DatabaseException e) {
        return false;
      }
      this.rightEntryNum=0;
      return true;
    }
    /*
     Read next page from the left source
     @return: true means successful. false means failure.
     */
    private boolean getNextLeftPage() {
      /* TODO */
      this.leftPage = this.leftIterator.next();
      try {
        leftHeader = PNLJOperator.this.getPageHeader(this.leftTableName, leftPage);
      } catch (DatabaseException e) {
        return false;
      }
      this.leftEntryNum=0;
      return true;
    }
    private Record getNextLeftRecordInPage() {
      /* TODO */
      return getRecord(leftPage, leftSchema, leftEntryNum, leftNo_of_Entries, leftHeader, leftEntrySize,
              leftHeaderSize);
    }

    private Record getNextRightRecordInPage() {
      /* TODO */
      return getRecord(rightPage, rightSchema, rightEntryNum, rightNo_of_Entries, rightHeader, rightEntrySize,
              rightHeaderSize);
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
