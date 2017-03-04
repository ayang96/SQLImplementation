package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 * An inner node of a B+ tree. An InnerNode header contains an `isLeaf` flag
 * set to 0 and the page number of the first child node (or -1 if no child
 * exists). An InnerNode contains InnerEntries.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class InnerNode extends BPlusNode {
    public static int headerSize = 5;       // isLeaf + pageNum of first child

    public InnerNode(BPlusTree tree) {
        super(tree, false);
        tree.incrementNumNodes();
        getPage().writeByte(0, (byte) 0);   // isLeaf = 0
        setFirstChild(-1);
    }

    public InnerNode(BPlusTree tree, int pageNum) {
        super(tree, pageNum, false);
        if (getPage().readByte(0) != (byte) 0) {
            throw new BPlusTreeException("Page is not Inner Node!");
        }
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    public int getFirstChild() {
        return getPage().readInt(1);
    }

    public void setFirstChild(int val) {
        getPage().writeInt(1, val);
    }

    /**
     * Finds the correct child of this InnerNode whose subtree contains the
     * given key.
     *
     * @param key the given key
     * @return page number of the child of this InnerNode whose subtree
     * contains the given key
     */
    public int findChildFromKey(DataBox key) {
        int keyPage = getFirstChild();  // Default keyPage
        List<BEntry> entries = getAllValidEntries();
        for (BEntry ent : entries) {
            if (key.compareTo(ent.getKey()) < 0) {
                break;
            }
            keyPage = ent.getPageNum();
        }
        return keyPage;
    }

    /**
     * Inserts a LeafEntry into the corresponding LeafNode in this subtree.
     *
     * @param ent the LeafEntry to be inserted
     * @return the InnerEntry to be pushed/copied up to this InnerNode's parent
     * as a result of this InnerNode being split, null otherwise
     */
    public InnerEntry insertBEntry(LeafEntry ent) {
        // Implement me!
        int childPageNo= findChildFromKey(ent.getKey());
        BPlusNode bp= getBPlusNode(this.getTree(), childPageNo);
        InnerEntry retEntry =bp.insertBEntry(ent);
        if (retEntry==null) {//no need to modify this inner node
            return null;
        }
        //Now we have insert this retEntry from children to this node

        if (this.hasSpace()){
            List<BEntry> entries = getAllValidEntries();
            entries.add(retEntry);
            Collections.sort(entries); //Sort all entries following Alternative 1 design
            this.overwriteBNodeEntries(entries);
            return null; //no split is needed
        }
        InnerEntry upEntry=splitNode(retEntry);
        return upEntry;
    }

    /**
     * Splits this InnerNode and returns the resulting InnerEntry to be
     * pushed/copied up to this InnerNode's parent as a result of the split.
     * The left node should contain d entries and the right node should contain
     * d entries.
     *
     * @param newEntry the BEntry that is being added to this InnerNode
     * @return the resulting InnerEntry to be pushed/copied up to this
     * InnerNode's parent as a result of this InnerNode being split
     */
    @Override
    public InnerEntry splitNode(BEntry newEntry) {
        // Implement me!
        BPlusTree tree = getTree();
        //tree.incrementNumNodes();

        // Implement me!
        // Move half of entries to the new leaf node
        List<BEntry> entries = getAllValidEntries();
        entries.add(newEntry);
        Collections.sort(entries);
        int midpoint=entries.size()/2;
        List<BEntry> entryLeft = new ArrayList<BEntry>();
        for (int i=0; i<midpoint; i++){
            entryLeft.add(entries.get(i));
        }
        this.overwriteBNodeEntries(entryLeft);

        BEntry midEntry=entries.get(midpoint);
        int midRight=midEntry.getPageNum();
        BPlusNode rightInnerNode= new InnerNode(tree); //it will allocate a page also
        ( (InnerNode) rightInnerNode).setFirstChild(midRight);

        List<BEntry> entryRight = new ArrayList<BEntry>();
        for (int i= midpoint+1; i<entries.size();i++){//start from midpoint+1
            entryRight.add(entries.get(i));
        }
        rightInnerNode.overwriteBNodeEntries(entryRight);

        //Push the middle key up. We donot leave a copy in this inner node
        InnerEntry upEntry= new InnerEntry(midEntry.getKey(), rightInnerNode.getPageNum());

        return upEntry;

    }
}
