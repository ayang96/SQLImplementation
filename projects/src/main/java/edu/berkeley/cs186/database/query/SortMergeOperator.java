package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;
import org.omg.CORBA.INTERNAL;

import java.lang.reflect.Array;
import java.util.*;
import java.lang.*;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  /**
  * An implementation of Iterator that provides an iterator interface for this operator.
  */
  private class SortMergeIterator implements Iterator<Record> {
    /* TODO: Implement the SortMergeIterator */
    private String leftTableName;
    private String rightTableName;
    private Iterator<Record> leftRecIterator;
    private Iterator<Record> rightRecIterator;
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
    // add these fields to memorize last equality comparsion
    private boolean lastEQFound;// true means there was an EQ match last time, more can be found
    private int rightEQStart;//starting entry number of the right table page

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      /* TODO */

      this.leftRecIterator = getLeftSource().iterator();
      this.rightRecIterator = getRightSource().iterator(); 
      leftTableName = "Temp SortMerge Left Table"; 
      rightTableName = "Temp SortMerge Right Table";
      SortMergeOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
      SortMergeOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
      sortTable(leftRecIterator, leftTableName, new LeftRecordComparator());
      sortTable(rightRecIterator, rightTableName, new RightRecordComparator() );

      this.leftIterator=SortMergeOperator.this.getPageIterator(this.leftTableName);
      this.leftIterator.next();//skip page 0
      this.rightIterator=SortMergeOperator.this.getPageIterator(this.rightTableName);
      this.rightIterator.next();//skip page 0
      leftRecord=nextRecord=rightRecord=null;
      leftPage= rightPage=null;
      leftNo_of_Entries=SortMergeOperator.this.getNumEntriesPerPage(this.leftTableName);
      rightNo_of_Entries=SortMergeOperator.this.getNumEntriesPerPage(this.rightTableName);
      leftHeader= rightHeader=null;
      leftEntryNum= rightEntryNum=0;//to be fetched next
      leftSchema=SortMergeOperator.this.getLeftSource().getOutputSchema();
      rightSchema=SortMergeOperator.this.getRightSource().getOutputSchema();
      leftHeaderSize=SortMergeOperator.this.getHeaderSize(this.leftTableName);
      rightHeaderSize=SortMergeOperator.this.getHeaderSize(this.rightTableName);
      leftEntrySize=SortMergeOperator.this.getEntrySize(this.leftTableName);
      rightEntrySize=SortMergeOperator.this.getEntrySize(this.rightTableName);

      this.getNextLeftPage();
      this.getNextRightPage();
      lastEQFound=false;
    }

    /* Read a database table and sort it, and then output
     */
    private void sortTable(Iterator<Record> iter, String tableName, Comparator cp){
	    ArrayList<Record> recordList=new ArrayList<Record>();
        Record r;
        DataBox keyValue;
        List<DataBox> listval;
        int hashpartval;
    	while (iter.hasNext()) {
          r=iter.next();
          recordList.add(r);
        }
	    Collections.sort(recordList,cp);
	    for (int i=0 ;i<recordList.size(); i++){
            r=recordList.get(i);
            try {
                SortMergeOperator.this.addRecord(tableName, r.getValues());
            } catch(DatabaseException e) {
                return; //somehow failed to continue.
            }

	    }
	    recordList.clear();
    }

    /**
    * Checks if there are more record(s) to yield
    *
    * @return true if this iterator has another record to yield, otherwise false
    */
    public boolean hasNext() {
        /* TODO */
        if (this.nextRecord!= null){
        	return true;
        }
        LeftRightRecordComparator cp=new LeftRightRecordComparator(); 
        boolean succ;
        //We follow SortMerge algorithm from Slide #60+ of the lecture
	    //We also follow the assumption of this project that once we find equal records, we donot need to backtrack to the previous page
   
	    if(lastEQFound==false){//we did not find equal pairs last time
	        succ= advanceLeftTable();//make sure leftRecord!=null
	        if(!succ) return false;
	        succ= advanceRightTable();//make sure rightRecord!=null
	        if(!succ) return false;
	        while(cp.compare(this.leftRecord, this.rightRecord)<0){//leftRecord <rightRecord
		        this.leftRecord=null;//drop the current one
		        succ= advanceLeftTable();//make sure leftRecord!=null
		        if(!succ) return false;
	        }
	        while(cp.compare(this.leftRecord, this.rightRecord)>0){//leftRecord >rightRecord
		        this.rightRecord=null;//drop the current one
		        succ= advanceRightTable();//make sure rightRecord!=null
		        if(!succ) return false;
	        }
	        lastEQFound=true;//we find an equal pair
            rightEQStart=this.rightEntryNum-1; //First we mark the start position of equal records
	    }
	    //Now we double check if the last pair built is a good pair
	    //if (cp.compare(leftRecord, rightRecord)==0){//leftRecord ==rightRecord
		//Loop for all equal left records
            	//while( cp.compare(this.leftRecord, this.rightRecord)==0){//leftRecord ==rightRecord
		//Loop for all equal right records
			 
	    List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
	    List<DataBox> rightValues = new ArrayList<DataBox>(this.rightRecord.getValues());
	    leftValues.addAll(rightValues);
	    this.nextRecord=new Record(leftValues);

	    //Prepare for the  next pair of equal records
	    this.rightRecord=null;//drop the current one
	    advanceRightTable();//advance within the same page, assuming no backtrack is needed
	    if(this.rightRecord==null || cp.compare(leftRecord, rightRecord)!=0){
		    //no right record is found within the same page.  We loop to next left record
		    this.leftRecord=null;//drop the current one
		    advanceLeftTable();
	    	this.rightEntryNum=rightEQStart; //reset  the right record pointer
		    this.rightRecord=null;//drop the current one
		    advanceRightTable();

	    }
	    if(leftRecord==null || rightRecord==null || cp.compare(leftRecord, rightRecord)!=0){
		//no pair is found
                lastEQFound=false;   //no longer available under the current equal condition
        }
	    return true;
    }

    /*
      @return next matched record from the current right record,  and the left list of matched records
     */
    //private void yieldOutput(){
         //DataBox leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
         //DataBox rightJoinValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
         //if (leftJoinValue.equals(rightJoinValue)) 
    //}


    /*Advance left record to next position
     * @return true means sucessful, otherwise failure.
     */

    private boolean advanceLeftTable() {
      /* TODO */
        while(this.leftRecord == null){
		    if(this.leftEntryNum<this.leftNo_of_Entries){//more to read this page
	  		    this.leftRecord=getNextLeftRecordInPage();
	  		    this.leftEntryNum++;
		    }else { //this page has no records
	       	      if(!getNextLeftPage()) //unsucessful
			     return false;
		    }
	    }
      return true;
    }
    /*
     Read next page from the right source
     @return: true means successful. false means failure.
     */
    private boolean getNextRightPage() {
        if(!this.rightIterator.hasNext()) return false;
        this.rightPage = this.rightIterator.next();
        try {
            rightHeader = SortMergeOperator.this.getPageHeader(this.rightTableName, rightPage);
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
      if(!this.leftIterator.hasNext()) return false;
      this.leftPage = this.leftIterator.next();
      try {
        leftHeader = SortMergeOperator.this.getPageHeader(this.leftTableName, leftPage);
      } catch (DatabaseException e) {
        return false;
      }
      this.leftEntryNum=0;
      return true;
    }
    /*Advance left record to next position
     * @return true means sucessful, otherwise failure.
     */
 
    private boolean advanceRightTable() {
      /* TODO */

        while(this.rightRecord == null){
		    if(this.rightEntryNum<this.rightNo_of_Entries){//more to read this page
	  		    this.rightRecord=getNextRightRecordInPage();
	  		    this.rightEntryNum++;
		    }else { //this page has no records
	       	      if(!getNextRightPage()) //unsucessful
			        return false;
		    }
	    }
      return true;
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
	      this.nextRecord=null;
              return r;
      }
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
    private class LeftRightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
            o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
