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

public class BNLJOperator extends JoinOperator {

  private int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class BNLJIterator implements Iterator<Record> {
    /* TODO: Implement the BNLJIterator */
    //Extra fields
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
    private int leftNo_of_Entries, rightNo_of_Entries;
    private Schema leftSchema,rightSchema;
    private int leftEntrySize, rightEntrySize, leftHeaderSize, rightHeaderSize;
    // For managing B-2 pages
    private ArrayList<Page> leftPageBlockList;
    private int nextPageIndex;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      /* TODO */
      if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) BNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (BNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator) BNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
        BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      /* TODO */
      //Initialize the iterator attributes. Notice BNLJOperator.this is a JoinOperator.
      this.leftIterator=BNLJOperator.this.getPageIterator(this.leftTableName);
      this.leftIterator.next();//skip page 0
      this.rightIterator=BNLJOperator.this.getPageIterator(this.rightTableName);
      this.rightIterator.next();//skip page 0
      leftRecord=nextRecord=rightRecord=null;
      leftPage= rightPage=null;
      leftNo_of_Entries=BNLJOperator.this.getNumEntriesPerPage(this.leftTableName);
      rightNo_of_Entries=BNLJOperator.this.getNumEntriesPerPage(this.rightTableName);
      leftHeader= rightHeader=null;
      leftEntryNum= rightEntryNum=0;//to be fetched next
      leftSchema=BNLJOperator.this.getLeftSource().getOutputSchema();
      rightSchema=BNLJOperator.this.getRightSource().getOutputSchema();
      leftHeaderSize=BNLJOperator.this.getHeaderSize(this.leftTableName);
      rightHeaderSize=BNLJOperator.this.getHeaderSize(this.rightTableName);
      leftEntrySize=BNLJOperator.this.getEntrySize(this.leftTableName);
      rightEntrySize=BNLJOperator.this.getEntrySize(this.rightTableName);

      leftPageBlockList=new ArrayList<Page>(BNLJOperator.this.numBuffers-2);
      this.getNextLeftPageBlock();
      this.getNextRightPage();


    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      /* TODO */
      if(BNLJOperator.this.numBuffers<3) //If not enough space to handle BNLJ
        return false;
      if (this.nextRecord != null) {
        return true;
      }
      while (true) {
        while(this.leftRecord == null) {
          if (! pageBlockHasNextRecord()) { //we have scanned all records of this left page block
            //we should set to read to the next page of right source
            if(this.rightIterator.hasNext()) {
              resetStartLeftPageBlock();
              if( !getNextRightPage())
                return false;
            } else {//nothing to scan from the right source, then we have to
              // increment pointer to the left source page
              if (this.leftIterator.hasNext()) {
                if( !getNextLeftPageBlock())
                  return false;
                try { //We start from page 0 for the right source
                  this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
                } catch (DatabaseException e) {
                  return false;
                }
                if( !getNextRightPage())
                  return false;

              } else //there is nothing to read any more.
                return false;
            }
          }  //Read left record
          this.leftRecord = getNextLeftRecordInBlock();

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
          DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
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
    /*
 Read next page from the right source
 @return: true means successful. false means failure.
 */
    private boolean getNextRightPage() {
      /* TODO */
      this.rightPage = this.rightIterator.next();
      try {
        rightHeader = BNLJOperator.this.getPageHeader(this.rightTableName, rightPage);
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
    private boolean getNextLeftPageBlock() {
      /* TODO */
      /*We assume the system buffer has B-2 pages where B=this.numBuffers
      these we go ahead to read B-2 pages if possible
       and assume the system buffer has these pages.
       */
      this.leftPageBlockList.clear();
      int currentPage=0;
      while(currentPage<BNLJOperator.this.numBuffers-2 && this.leftIterator.hasNext()){
        this.leftPageBlockList.add(this.leftIterator.next());
        currentPage++;
      }
      this.leftPage = leftPageBlockList.get(0);
      nextPageIndex=1;
      try {
        leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, leftPage);
      } catch (DatabaseException e) {
        return false;
      }
      this.leftEntryNum=0;
      return true;
    }
    /*
      Check if this left page block has additional records
     */
    private boolean pageBlockHasNextRecord(){
      if(this.leftEntryNum<this.leftNo_of_Entries|| this.nextPageIndex<this.leftPageBlockList.size())
        return true;
      return false;
    }
    /*
      Reset the record read pointer starting position for left page block
     */
    private boolean resetStartLeftPageBlock(){
      this.leftPage = leftPageBlockList.get(0);
      nextPageIndex=1;
      try {
        leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, leftPage);
      } catch (DatabaseException e) {
        return false;
      }
      this.leftEntryNum=0;
      return true;
    }
    private Record getNextLeftRecordInBlock() {
      /* TODO */
      if(this.leftEntryNum>=this.leftNo_of_Entries && this.nextPageIndex<this.leftPageBlockList.size()) {
        this.leftPage = leftPageBlockList.get(nextPageIndex);
        nextPageIndex++;
        try {
          leftHeader = BNLJOperator.this.getPageHeader(this.leftTableName, leftPage);
        } catch (DatabaseException e) {
          return null;
        }
        this.leftEntryNum = 0;
      }

      Record r = getRecord(leftPage, leftSchema, leftEntryNum, leftNo_of_Entries, leftHeader, leftEntrySize,
                leftHeaderSize);
      this.leftEntryNum++;
      return r;
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
