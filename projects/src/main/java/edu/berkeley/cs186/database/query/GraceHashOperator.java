package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.stats.TableStats;


public class GraceHashOperator extends JoinOperator {

  private int numBuffers;

  public GraceHashOperator(QueryOperator leftSource,
                           QueryOperator rightSource,
                           String leftColumnName,
                           String rightColumnName,
                           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.GRACEHASH);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private String[] leftPartitions;
    private String[] rightPartitions;
    /* TODO: Implement the GraceHashOperator */
    //Add these fileds

    private ArrayList< HashMap<Integer,List<Record>>> hashTableList;
    private int nextPartitionIndex;
    private Iterator<Record> rightPartitionIterator;
    private LinkedList<Record>   leftMatchedList; //This is a list of left source records matched current right record
    private Record nextRecord;   //next recrd matched, return for next() call
    private Record rightRecord;  // next record scanned from the right source




    public GraceHashIterator() throws QueryPlanException, DatabaseException {
      this.leftIterator = getLeftSource().iterator();
      this.rightIterator = getRightSource().iterator();
      leftPartitions = new String[numBuffers - 1];
      rightPartitions = new String[numBuffers - 1];
      String leftTableName;
      String rightTableName;
      for (int i = 0; i < numBuffers - 1; i++) {
        leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
        rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
        GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
        GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
        leftPartitions[i] = leftTableName;
        rightPartitions[i] = rightTableName;
      }
      /* TODO */
      partition(leftIterator,leftPartitions);
      partition(rightIterator,rightPartitions);

      hashTableList=new ArrayList<HashMap<Integer, List<Record>>>();
      for (int i=0; i<numBuffers-1; i++)
        hashTableList.add(new HashMap<Integer, List<Record>>());
      rightPartitionIterator=null;
      nextPartitionIndex=0;
      buildNextHashTable();
    }

    /* Partition a database table into B-1 parittions where B=this.numBuffers.
     */
    private void partition(Iterator<Record> iter, String[] tableName){
        Record r;
        DataBox keyValue;
        List<DataBox> listval;
        int hashpartval;
    	while (iter.hasNext()) {
          r=iter.next();
          listval=r.getValues();
          keyValue=listval.get(GraceHashOperator.this.getLeftColumnIndex());
          hashpartval=keyValue.hashCode() % (GraceHashOperator.this.numBuffers-1);
          try {
            GraceHashOperator.this.addRecord(tableName[hashpartval], listval);
          } catch(DatabaseException e){
            return; //somehow failed to continue.
          }
        }
    }
    /* Build a hashtable for one partition of the left database table.
     */
    private void build1LeftHashTable(String tableName){
      Iterator<Record> iter;
      try {
        iter = GraceHashOperator.this.getTableIterator(tableName);
      }catch(DatabaseException e){
        return;
      }
      Record r;
      DataBox key;
      List<DataBox> listval;
      List<Record> rlist;
      HashMap<Integer, List<Record>> hmap;
      int hashval,hashpartval;
      while (iter.hasNext()) {
        r=iter.next();
        listval=r.getValues();
        key=listval.get(GraceHashOperator.this.getLeftColumnIndex());
        hashval=key.hashCode();
        hashpartval=hashval% (GraceHashOperator.this.numBuffers-1);
        hmap=  hashTableList.get(hashpartval);
        rlist= hmap.get(hashval);
        if(rlist==null){
          rlist=new ArrayList<Record>();
          hmap.put(hashval,rlist);
        }
        rlist.add(r);
      }
    }

    /*Load next partition of left table, and build its hashtable, and then
      probe the corresponding partition of the right table
      @return  true means successful. false means failure.
     */
     private boolean buildNextHashTable() {
       if (nextPartitionIndex >=GraceHashOperator.this.numBuffers - 1)
          return false;
       build1LeftHashTable(leftPartitions[nextPartitionIndex]);
       try {
         rightPartitionIterator =
                 GraceHashOperator.this.getTableIterator(rightPartitions[nextPartitionIndex]);
       } catch (DatabaseException e) {
         return false;
       }
       nextPartitionIndex++;
       leftMatchedList=null;
       rightRecord=null;
       nextRecord=null;
       return true;
     }
    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      /* TODO */

      Record r;
      DataBox key;
      List<DataBox> listval;
      List<Record> rlist;
      HashMap<Integer, List<Record>> hmap;
      int hashval, hashpartval;
      if(GraceHashOperator.this.numBuffers<3) //If not enough space to handle BNLJ
          return false;
      if(rightPartitionIterator==null)
          return false; //something is wrong
      if (this.nextRecord != null) {
          return true;
      }
      while(true) {     //Read next record from the right partition if needed
        if (rightRecord != null && leftMatchedList != null) {
          this.nextRecord = getNextMatched();
          if (this.nextRecord!=null)
              return true;
        }

        while (this.rightRecord == null && this.rightPartitionIterator.hasNext()) {
          this.rightRecord = rightPartitionIterator.next();
        }
        if (this.rightRecord == null) {    //Still null, it means no more records in this partition.
          //Load next partition
          boolean succ=buildNextHashTable();
          if(!succ)  //fail to build next one
            return false;
        }else {   //Now we have a recrod from the right source
          listval = this.rightRecord.getValues();
          key = listval.get(GraceHashOperator.this.getLeftColumnIndex());
          hashval = key.hashCode();
          hashpartval=hashval% (GraceHashOperator.this.numBuffers - 1);
          hmap = hashTableList.get(hashpartval);
          if (hmap.containsKey(hashval) && hmap.get(hashval) != null)
            this.leftMatchedList = new LinkedList<Record>(hmap.get(hashval));  //Duplicate the matched list
          else {//nothing matched
            this.rightRecord = null;//useless, no match
            this.leftMatchedList = null;
          }
        }

      }
    }

    /*
    @return next matched record from the current right record,  and the left list of matched records
     */
    private Record getNextMatched(){
       Record leftRecord;
       while(!leftMatchedList.isEmpty()) {
         leftRecord = this.leftMatchedList.remove();

         DataBox leftJoinValue = leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
         DataBox rightJoinValue = this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
         if (leftJoinValue.equals(rightJoinValue)) {
           List<DataBox> leftValues = new ArrayList<DataBox>(leftRecord.getValues());
           List<DataBox> rightValues = new ArrayList<DataBox>(this.rightRecord.getValues());
           leftValues.addAll(rightValues);
           return (new Record(leftValues));
         }
       }
       this.rightRecord=null;//useless as nothing matched
       this.leftMatchedList=null;
       return null;//nothing matched.
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
