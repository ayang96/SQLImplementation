package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.databox.*;

import java.util.*;
import java.nio.file.Paths;

/**
 * A B+ tree. Allows the user to add, delete, search, and scan for keys in an
 * index. A BPlusTree has an associated page allocator. The first page in the
 * page allocator is a header page that serializes the search key data type,
 * root node page, and first leaf node page. Each subsequent page is a
 * BPlusNode, specifically either an InnerNode or LeafNode. Note that a
 * BPlusTree can have duplicate keys that appear across multiple pages.
 *
 * Properties:
 * allocator: PageAllocator for this index
 * keySchema: DataBox for this index's search key
 * rootPageNum: page number of the root node
 * firstLeafPageNum: page number of the first leaf node
 * numNodes: number of BPlusNodes
 */
public class BPlusTree {
    public static final String FILENAME_PREFIX = "db";
    public static final String FILENAME_EXTENSION = ".index";

    protected PageAllocator allocator;
    protected DataBox keySchema;
    private int rootPageNum;
    private int firstLeafPageNum;
    private int numNodes;

    /**
     * This constructor is used for creating an empty BPlusTree.
     *
     * @param keySchema the schema of the index key
     * @param fName the filename of where the index will be built
     */
    public BPlusTree(DataBox keySchema, String fName) {
        this(keySchema, fName, FILENAME_PREFIX);
    }

    public BPlusTree(DataBox keySchema, String fName, String filePrefix) {
        String pathname = Paths.get(filePrefix, fName + FILENAME_EXTENSION).toString();
        this.allocator = new PageAllocator(pathname, true);
        this.keySchema = keySchema;
        int headerPageNum = this.allocator.allocPage();
        assert(headerPageNum == 0);
        this.numNodes = 0;
        BPlusNode root = new LeafNode(this);
        this.rootPageNum = root.getPageNum();
        this.firstLeafPageNum = rootPageNum;
        writeHeader();
    }

    /**
     * This constructor is used for loading a BPlusTree from a file.
     *
     * @param fName the filename of a preexisting BPlusTree
     */
    public BPlusTree(String fName) {
        this(fName, FILENAME_PREFIX);
    }

    public BPlusTree(String fName, String filePrefix) {
        String pathname = Paths.get(filePrefix, fName + FILENAME_EXTENSION).toString();
        this.allocator = new PageAllocator(pathname, false);
        this.readHeader();
    }

    public void incrementNumNodes() {
        this.numNodes++;
    }

    public void decrementNumNodes() {
        this.numNodes--;
    }

    public int getNumNodes() {
        return this.numNodes;
    }

    /**
     * Perform a sorted scan.
     * The iterator should return all RecordIDs, starting from the beginning to
     * the end of the index.
     *
     * @return Iterator of all RecordIDs in sorted order
     */
    public Iterator<RecordID> sortedScan() {
        BPlusNode rootNode = BPlusNode.getBPlusNode(this, rootPageNum);
        return new BPlusIterator(rootNode);
    }

    /**
     * Perform a range search beginning from a specified key.
     * The iterator should return all RecordIDs, starting from the specified
     * key to the end of the index.
     *
     * @param keyStart the key to start iterating from
     * @return Iterator of RecordIDs that are equal to or greater than keyStart
     * in sorted order
     */
    public Iterator<RecordID> sortedScanFrom(DataBox keyStart) {
        BPlusNode root = BPlusNode.getBPlusNode(this, rootPageNum);
        return new BPlusIterator(root, keyStart, true);
    }

    /**
     * Perform an equality search on the specified key.
     * The iterator should return all RecordIDs that match the specified key.
     *
     * @param key the key to match
     * @return Iterator of RecordIDs that match the given key
     */
    public Iterator<RecordID> lookupKey(DataBox key) {
        BPlusNode root = BPlusNode.getBPlusNode(this, rootPageNum);
        return new BPlusIterator(root, key, false);
    }

    /**
     * Insert a (Key, RecordID) tuple.
     *
     * @param key the key to insert
     * @param rid the RecordID of the given key
     */
    public void insertKey(DataBox key, RecordID rid) {
        // Implement me!
        //First find the leaf node that should contain this key
        BPlusNode root;
        if (this.rootPageNum== this.firstLeafPageNum) {
            //Root is a leaf during the initial stage
            root = new LeafNode(this, this.rootPageNum);
        } else {
            root=new InnerNode(this,this.rootPageNum);
        }
        LeafEntry le=new LeafEntry(key,rid);
        InnerEntry ie=root.insertBEntry(le);
        if(ie!=null) {//root needs a split
            InnerNode newroot = new InnerNode(this);
            newroot.setFirstChild(this.rootPageNum);
            //This new root has one entry pf key+pointer
            List<BEntry> myList = new ArrayList<BEntry>();
            myList.add(ie);
            newroot.overwriteBNodeEntries(myList);
            this.updateRoot(newroot.getPageNum());
            //this.incrementNumNodes(); This function call is not necessary because InnerNode(this) will do that
        }

    }

    /**
     * Delete an entry with the matching key and RecordID.
     *
     * @param key the key to be deleted
     * @param rid the RecordID of the key to be deleted
     */
    public boolean deleteKey(DataBox key, RecordID rid) {
        /* You will not have to implement this in this project. */
        throw new BPlusTreeException("BPlusTree#DeleteKey Not Implemented!");
    }

    /**
     * Perform an equality search on the specified key.
     *
     * @param key the key to lookup
     * @return true if the key exists in this BPlusTree, false otherwise
     */
    public boolean containsKey(DataBox key) {
        return lookupKey(key).hasNext();
    }

    /**
     * Return the number of pages.
     *
     * @return the number of pages.
     */
    public int getNumPages() {
        return this.allocator.getNumPages();
    }

    /**
     * Update the root page.
     *
     * @param pNum the page number of the new root node
     */
    protected void updateRoot(int pNum) {
        this.rootPageNum = pNum;
        writeHeader();
    }

    private void writeHeader() {
        Page headerPage = allocator.fetchPage(0);
        int bytesWritten = 0;

        headerPage.writeInt(bytesWritten, this.rootPageNum);
        bytesWritten += 4;

        headerPage.writeInt(bytesWritten, this.firstLeafPageNum);
        bytesWritten += 4;

        headerPage.writeInt(bytesWritten, keySchema.type().ordinal());
        bytesWritten += 4;

        if (this.keySchema.type().equals(DataBox.Types.STRING)) {
            headerPage.writeInt(bytesWritten, this.keySchema.getSize());
            bytesWritten += 4;
        }
        headerPage.flush();
    }

    private void readHeader() {
        Page headerPage = allocator.fetchPage(0);

        int bytesRead = 0;

        this.rootPageNum = headerPage.readInt(bytesRead);
        bytesRead += 4;

        this.firstLeafPageNum = headerPage.readInt(bytesRead);
        bytesRead += 4;

        int keyOrd = headerPage.readInt(bytesRead);
        bytesRead += 4;
        DataBox.Types type = DataBox.Types.values()[keyOrd];

        switch(type) {
            case INT:
                this.keySchema = new IntDataBox();
                break;
            case STRING:
                int len = headerPage.readInt(bytesRead);
                this.keySchema = new StringDataBox(len);
                break;
            case BOOL:
                this.keySchema = new BoolDataBox();
                break;
            case FLOAT:
                this.keySchema = new FloatDataBox();
                break;
        }
    }

    /**
     * A BPlusIterator provides several ways of iterating over RecordIDs stored
     * in a BPlusTree.
     */
    private class BPlusIterator implements Iterator<RecordID> {
        // Implement me!
        Stack <BPlusNode> listParents; //Postorder list of parents that should be visited next
        int currentPage; //current page number in a  key-sorted order
        Iterator<RecordID> lscan;//current iterator of RecordID within the current leaf node
        List<BEntry> siblingList; //Next sibling should be visited
        int siblingCursor; //Current position in the sibling list
        DataBox keyCompare; //Key to compare for equality/range search, null if nothing to compare
        boolean rangeSearch; //True if it is a range search

        Page pageref;

        /**
         * Construct an iterator that performs a sorted scan on this BPlusTree
         * tree.
         * The iterator should return all RecordIDs, starting from the
         * beginning to the end of the index.
         *
         * @param root the root node of this BPlusTree
         */
        public BPlusIterator(BPlusNode root) {
            // Implement me!
            keyCompare=null; //nothing to compare
            rangeSearch=false;
            lscan=null;//nothing to see for now
            listParents=new Stack<BPlusNode>();
            pushAllLeft(root);
            siblingList=null;
            siblingCursor=0;//nothing to see from the sibling list
        }

        /**
         * Construct an iterator that performs either an equality or range
         * search with a specified key.
         * If @param scan is true, the iterator should return all RecordIDs,
         * starting from the specified key to the end of the index.
         * If @param scan is false, the iterator should return all RecordIDs
         * that match the specified key.
         *
         * @param root the root node of this BPlusTree
         * @param key the specified key value
         * @param scan if true, do a range search; else, equality search
         */
        public BPlusIterator(BPlusNode root, DataBox key, boolean scan) {
            // Implement me!
            keyCompare=key;
            rangeSearch=scan;
            lscan=null;//nothing to see for now
            listParents=new Stack<BPlusNode>();
            pushAllLeft(root);
            siblingList=null;
            siblingCursor=0;//nothing to see from the sibling list

        }
        /*
        Push all parents visited into a stack that allows a post-order visitation
         */
        private void pushAllLeft(BPlusNode node) {
            BPlusNode bp=node;
            while (!bp.isLeaf()){ //Push to the left nodes as much as possible
                this.listParents.add(bp);
                int leftNo= ((InnerNode) bp).getFirstChild(); //get the left child
                if(leftNo<=0|| BPlusTree.this.numNodes<leftNo){//somethind is wrong
                    throw new NoSuchElementException();
                }
                bp=BPlusNode.getBPlusNode(BPlusTree.this,leftNo);

            }
            //Now this node is a leaf
            LeafNode lnode=(LeafNode)bp;
            if(keyCompare==null)
                this.lscan= lnode.scan();
            else {
                if(rangeSearch)
                    this.lscan=lnode.scanFrom(keyCompare);
                else //now it is an equal key lookup
                    if(lnode.containsKey(keyCompare))
                        this.lscan=lnode.scanForKey(keyCompare);
                    else this.lscan=null;//nothing is found in this leave node
            }
        }
        /*prepare for next scan
        * @return true if next scan is prepared successfully, there is a next element available
        * false means nothing is available anymore.
        */
        private boolean prepareScan() {
            BPlusNode bp;
            BEntry be;
            int childNo;
            lscan=null; //current scan list is not good anymore
            while(true) {
                while (siblingList != null && siblingCursor < siblingList.size()) {

                    be = siblingList.get(siblingCursor);
                    siblingCursor++;
                    childNo = be.getPageNum();
                    if(keyCompare!=null && rangeSearch ==false) { // exact search
                        if(be.getKey().compareTo(keyCompare)>0){// the current key is bigger than search key
                            return false;
                        }
                    }
                    bp = BPlusNode.getBPlusNode(BPlusTree.this, childNo);
                    pushAllLeft(bp);
                    if (lscan != null && lscan.hasNext()) {
                        return true;
                    }
                }
                //We have visited all siblings, we need to move up
                if (this.listParents.empty())
                    return false; //nothing to see
                bp = this.listParents.pop();
                siblingList=bp.getAllValidEntries();
                siblingCursor=0; //We have already visited the left most, start from here

            }
        }
        /**
         * Confirm if iterator has more RecordIDs to return.
         *
         * @return true if there are still RecordIDs to be returned, false
         * otherwise
         */
        public boolean hasNext() {
            // Implement me!t
            BPlusNode bp;
            BEntry be;
            int childNo;
            if(lscan!=null&& lscan.hasNext())
                return true;

            //Nothing to see in the current leaf node
            //If the current leaf has no more record, we will look for next sibling leaf node in the B+ tree
            return prepareScan();

        }

        /**
         * Yield the next RecordID of this iterator.
         *
         * @return the next RecordID
         * @throws NoSuchElementException if there are no more RecordIDs to
         * yield
         */
        public RecordID next() {
            // Implement me!
            boolean tryNext;
            if(lscan!=null&&lscan.hasNext())
                return lscan.next();
            tryNext=prepareScan();
            if(tryNext)
                return lscan.next();//we know parepScan must have done successfully.
            else
                throw new NoSuchElementException();

        }

        public void remove() {
            /* You will not have to implement this in this project. */
            throw new UnsupportedOperationException();
        }
    }
}
