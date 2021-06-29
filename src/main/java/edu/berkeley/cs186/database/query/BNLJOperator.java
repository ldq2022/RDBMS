package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinOperator.JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;

        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;

        // The current record on the left page
        private Record leftRecord = null;

        private Record rightRecord = null;

        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement

            // private BacktrackingIterator<Page> leftIterator;
            // private BacktrackingIterator<Page> rightIterator;
            //
            // private BacktrackingIterator<Record> leftRecordIterator = null;
            // private BacktrackingIterator<Record> rightRecordIterator = null;
            //
            // private Record leftRecord = null;
            // private Record nextRecord = null;

            if(!this.leftIterator.hasNext()) {  // EOF: there's no more page - so no more block
                this.leftRecordIterator = null;
                this.leftRecord = null;
            } else {
                this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), this.leftIterator, numBuffers - 2);
                if (this.leftRecordIterator.hasNext()) {
                    this.leftRecord = this.leftRecordIterator.next();
                }
                leftRecordIterator.markPrev();
            }




        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the left relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement

            if(!this.rightIterator.hasNext()) {  // EOF: there's no more page
                this.rightRecordIterator = null;
                this.rightRecord = null;
            } else {
                this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), this.rightIterator,  1);
                if (this.rightRecordIterator.hasNext()) {
                    this.rightRecord = this.rightRecordIterator.next();
                }
                rightRecordIterator.markPrev();
            }

        }







        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            // TODO(proj3_part1): implement

            if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch"); }
            this.nextRecord = null;

            while (!hasNext()) { // which means nextRecord is still null --- gonna keep looping until we find a match between left and right

                // if both left and right don't reach end, we can actually do the join between left block and right page:
                // this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                if (rightRecord != null) {
                    DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        this.nextRecord = joinRecords(this.leftRecord, rightRecord);
                    }
                    this.rightRecord = this.rightRecordIterator.hasNext() ? this.rightRecordIterator.next() : null;
                    continue;
                }

                if (!leftIterator.hasNext() && !rightIterator.hasNext() &&
                    !leftRecordIterator.hasNext() && !rightRecordIterator.hasNext()) { // 1. when both left and right reach the end of relations
                    throw new NoSuchElementException("No new record to fetch");
                }

                if (!leftRecordIterator.hasNext() && !rightIterator.hasNext() && !rightRecordIterator.hasNext()) { // 3. when right reaches the end of relation, left reaches end of block, advance left block and reset right
                    fetchNextLeftBlock();
                    resetRightRelation();   // we not only have to advance left block, but also reset our whole right relation
                    fetchNextRightPage();
                    continue;
                }

                if (!leftRecordIterator.hasNext() && rightIterator.hasNext() && !rightRecordIterator.hasNext()) { // 2. left reaches end of block, right reaches end of page, advance right page
                    fetchNextRightPage();
                    resetLeftBlock();
                    continue;
                }

                if (leftRecordIterator.hasNext() && !rightRecordIterator.hasNext()) {  // (most common one) when right reaches end of the page, need to advance left record
                    leftRecord = leftRecordIterator.next();
                    resetRightPage();
                    continue;
                }

            }

        }




        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }






        // helper 1
        private void resetRightRelation() {
            this.rightIterator.reset();
        }


        // helper 2
        private void resetRightPage() {
            this.rightRecordIterator.reset();
            assert(rightRecordIterator.hasNext());
            rightRecord = rightRecordIterator.next();
        }

        // helper 3
        private void resetLeftBlock() {
            this.leftRecordIterator.reset();
            assert(leftRecordIterator.hasNext());
            leftRecord = leftRecordIterator.next();
        }








        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
