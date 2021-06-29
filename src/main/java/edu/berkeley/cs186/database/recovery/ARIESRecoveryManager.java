
package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
//import sun.rmi.runtime.Log;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;


/**
 * Implementation of ARIES.
 */

public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

/**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */

    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

/**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */

    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

/**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */

    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }


/**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */

    @Override
    public long commit(long transNum) {
        // Create LSN of committing record
        // For clarity, pull out the TransactionTableEntry
        TransactionTableEntry entry = transactionTable.get(transNum);
        long commitLSN = getCommitLSN(entry, transNum);
        // Commit records need to be flushed to disk!!!
        pageFlushHook(commitLSN);

        // Set the status of the transaction, see enum STATUS
        entry.transaction.setStatus(Transaction.Status.COMMITTING);
        // Update this transactionTable's lastLSN to the newly created one, reflecting the commit status
        entry.lastLSN = commitLSN;

        return commitLSN;
    }

/**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */

    @Override
    public long abort(long transNum) {
        // You know the drill
        TransactionTableEntry entry = transactionTable.get(transNum);
        long abortLSN = getAbortLSN(entry, transNum);

        entry.transaction.setStatus(Transaction.Status.ABORTING);
        entry.lastLSN = abortLSN;
        return abortLSN;
    }

/**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */

    @Override
    public long end(long transNum) {
        // Handling the rollback case for aborting transactions
        TransactionTableEntry entry = transactionTable.get(transNum);
        Transaction.Status status = entry.transaction.getStatus();

        if (status == Transaction.Status.ABORTING) {

            long startUndoLSN = getLastLSN(transNum);
            LogRecord LR = fetchLogRecord(startUndoLSN);

            // For each record that this aborting transaction wrote to
            while (LR.getPrevLSN().isPresent() || LR.getUndoNextLSN().isPresent()) {
                if (LR.isUndoable()) {
                    Pair<LogRecord, Boolean> CLR = LR.undo(LR.getPrevLSN().get());
                    LogRecord record = CLR.getFirst();
                    logManager.appendToLog(record);
                    entry.lastLSN = record.LSN;
                    if (CLR.getSecond()) {
                        pageFlushHook(CLR.getFirst().LSN);
                    }
                    CLR.getFirst().redo(diskSpaceManager, bufferManager);
                }


                if (LR.getUndoNextLSN().isPresent()) {
                    LR = fetchLogRecord(LR.getUndoNextLSN().get());
                } else {
                    LR = fetchLogRecord(LR.getPrevLSN().get());
                }

            }
        }

        long endLSN = getEndLSN(entry, transNum);
        entry.transaction.setStatus(Transaction.Status.COMPLETE);
        // we're done with this record, yay!
        transactionTable.remove(transNum);
        return endLSN;
    }

/**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */

    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

/**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */

    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

/**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */

    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        // Arguments to before and after params are guaranteed to be the same length
        assert (before.length == after.length);
        long lastLSN = getLastLSN(transNum);
        // Need to store the new LSN to update transactionTable later
        long newLastLSN;
        UpdatePageLogRecord updatePageLog;


        // If number of bytes written is too large:
        if (before.length > (bufferManager.EFFECTIVE_PAGE_SIZE / 2)) {
            // Split the page update into two records
            // Emit the first record to log corresponding to everything BEFORE the write
            logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, before, null));
            // Emit the second record to log corresponding to everything AFTER the write
            updatePageLog = new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, null, after);
        } else {
            // Page update is over a smaller region, so write to log in a single update record
            updatePageLog = new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, before, after);
        }
        newLastLSN = logManager.appendToLog(updatePageLog);
        // Update transactionTable's lastLSN to the new one
        setLastLSN(transNum, newLastLSN);
        // We still need to maintain the dirty page table & transaction table
        transactionTable.get(transNum).touchedPages.add(pageNum);
        // We always consider the page dirty since a write ocurred!!!!!
        dirtyPageTable.putIfAbsent(pageNum, newLastLSN);
        //addToDirtyTable(transNum, pageNum);
        return newLastLSN;
    }
/**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */

    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }
/**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */

    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

/**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */

    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

/**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */

    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }
/**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */

    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

/**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */

    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

/**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */

    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        HashSet<LogType> pageLogTypes = getPageLogTypes();

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);
        long lastLSN = transactionEntry.lastLSN;
        long undoLSN = transactionEntry.lastLSN;
        // Trying to collapse wordy calls into descriptive helpers
        //LogRecord LR = fetchLogRecord(lastLSN);
        // While undoLSN != savepoint, undo changes by the transaction
        while (undoLSN != LSN) {
            LogRecord LR = fetchLogRecord(undoLSN);
            if (LR.isUndoable()) {
                Pair<LogRecord, Boolean> CLR = LR.undo(lastLSN);
                LogRecord record = CLR.getFirst();
                // Update lastLSN
                lastLSN = logManager.appendToLog(record);

                if (CLR.getSecond()) {
                    pageFlushHook(record.LSN);
                }

                record.redo(diskSpaceManager, bufferManager);

                // Updates to dirty page table must be made
                if (pageLogTypes.contains(record.type)) {
                    long pageNum = record.getPageNum().get();
                    if (record.type == LogType.UPDATE_PAGE || record.type == LogType.UNDO_UPDATE_PAGE) {
                        dirtyPageTable.putIfAbsent(pageNum, lastLSN);
                    }
                    // Because CLR will undo its dirty changes!
                    else if (record.type == LogType.UNDO_ALLOC_PAGE) {
                        dirtyPageTable.remove(pageNum);
                    }
                }
            }


            /// SUPER IMPORTANT: check for getUndoNextLSN FIRST!
            if (LR.getUndoNextLSN().isPresent()) {
                undoLSN = LR.getUndoNextLSN().get();
            }
            // Then check for getPrevLSN
            else if (LR.getPrevLSN().isPresent()) {
                undoLSN = LR.getPrevLSN().get();
            }
            else {
                // Added this here just for clarity
                undoLSN = LSN;
            }
        }
    }
/**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */

    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        // This is a looooong method, maybe figure out how to condense?
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // Iterate through dirtyPageTable's entries
        for (Map.Entry<Long, Long> dptEntry : dirtyPageTable.entrySet()) {
            if (dpt.size() < bufferManager.EFFECTIVE_PAGE_SIZE) {
                // copy dirtyPageTable entries to dpt map
                dpt.put(dptEntry.getKey(), dptEntry.getValue());
            } else {
                // If copying current record will cause end checkpoint to be too large (always relative to BuffMan space)
                // then append an end checkpoint record with DPT entries so far to log
                logManager.appendToLog(new EndCheckpointLogRecord(dpt, txnTable, touchedPages));
                // reset DPT to continue setting end checkpoints
                dpt = new HashMap<>();
            }
        }

        // Iterate through transaction table, copying status, lastLSN, and touched pages
        // Break up as needed with EndCheckPoints
        // Should the iteration over status&LSN happen at the same time as touched pages??
        for (Map.Entry<Long, TransactionTableEntry> transactionEntry : transactionTable.entrySet()) {
            Long key = transactionEntry.getKey();
            TransactionTableEntry value = transactionEntry.getValue();
            if (txnTable.size() >= bufferManager.EFFECTIVE_PAGE_SIZE) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt = new HashMap<>();
                txnTable = new HashMap<>();
            }
            Pair<Transaction.Status, Long> statusAndLSN = new Pair(value.transaction.getStatus(),
                    value.lastLSN);
            txnTable.put(key, statusAndLSN);
        }

        for (Map.Entry<Long, TransactionTableEntry> transactionEntry : transactionTable.entrySet()) {
            Long key = transactionEntry.getKey();
            TransactionTableEntry value = transactionEntry.getValue();
            if (txnTable.size() >= bufferManager.EFFECTIVE_PAGE_SIZE) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt = new HashMap<>();
                txnTable = new HashMap<>();
                touchedPages = new HashMap<>();
            }
            List<Long> copyTouchedPages = new ArrayList<>();
            copyTouchedPages.addAll(value.touchedPages);
            //assert(!copyTouchedPages.isEmpty());
            touchedPages.put(key, copyTouchedPages);
        }

        // Checking if the end checkpoint will fit in one record
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                            dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    public void setLastLSN(long transNum, long LSN) {
        transactionTable.get(transNum).lastLSN = LSN;
    }

    public long getLastLSN(long transNum) {
        return transactionTable.get(transNum).lastLSN;
    }

    public long getCommitLSN(TransactionTableEntry e, long transNum) {
        LogRecord commit = new CommitTransactionLogRecord(transNum, e.lastLSN);
        return logManager.appendToLog(commit);
    }

    public long getAbortLSN(TransactionTableEntry e, long transNum) {
        LogRecord abort = new AbortTransactionLogRecord(transNum, e.lastLSN);
        return logManager.appendToLog(abort);
    }

    public long getEndLSN(TransactionTableEntry e, long transNum) {
        LogRecord end = new EndTransactionLogRecord(transNum, e.lastLSN);
        return logManager.appendToLog(end);
    }

    public LogRecord fetchLogRecord(long LSN) {
        return logManager.fetchLogRecord(LSN);
    }

    public void handleUndoableLR(LogRecord LR, long LSN) {
        Pair<LogRecord, Boolean> CLR = LR.undo(LSN);
        if (CLR.getSecond()) {
            logManager.flushToLSN(CLR.getFirst().LSN);
        }
        logManager.appendToLog(CLR.getFirst());
        CLR.getFirst().redo(diskSpaceManager, bufferManager);
    }

    public void addTouchedPage(long transNum, long pageNum) {
        if (!transactionTable.get(transNum).touchedPages.contains(pageNum)) {
            transactionTable.get(transNum).touchedPages.add(pageNum);
        }
    }

    public void addToDirtyTable(long transNum, long pageNum) {
        long LSN = getLastLSN(transNum);
        if (!dirtyPageTable.containsKey(pageNum)) {
            dirtyPageTable.put(pageNum, LSN);
        }
    }

    public void removeFromDirtyTable(long pageNum) {
        if (dirtyPageTable.containsKey(pageNum)) {
            dirtyPageTable.remove(pageNum);
        }
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////
/**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */

    @Override
    public Runnable restart() {
        restartAnalysis();
        restartRedo();

        // This is so weird...
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (!dirty) {
                dirtyPageTable.remove(pageNum);
            }
        });

        // Return a Runnable that performs the undo phase and checkpoint,
        // instead of performing those actions immediately
        // See https://www.geeksforgeeks.org/runnable-interface-in-java/
        class RunnableImplementation implements Runnable {
            @Override
            public void run() {
                restartUndo();
                checkpoint();
            }
        }
        return new RunnableImplementation();
    }

/**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */

    void restartAnalysis() {
        // Read master record
        LogRecord record = fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // HashSet will be the quickest way to see if a logType corresponds
        // to a transaction, a page, a status, or checkpoint
        // See this stackoverflow for reasoning on HashSet vs LogType[]:
        // https://stackoverflow.com/questions/4936819/java-check-if-enum-contains-a-given-string/4936895
        HashSet<LogType> transactionLogTypes = getTransactionLogTypes();
        // Special handling for pages, that's why we need this
        HashSet<LogType> pageLogTypes = getPageLogTypes();
        HashSet<LogType> statusLogTypes = getStatusLogTypes();
        HashSet<LogType> checkpointLogTypes = getCheckpointLogTypes();

        // Return iterator over Log Records starting from the Master
        Iterator<LogRecord> LRIterator = logManager.scanFrom(LSN);


        while (LRIterator.hasNext()) {
            LogRecord LR = LRIterator.next();
            LogType LRType = LR.getType();

            // Case 1: Log Record involves a transaction operation
            if (LR.getTransNum().isPresent()) {
            //if (transactionLogTypes.contains(LRType)) {
                Long transNum = LR.getTransNum().get();
                Transaction currTransaction = newTransaction.apply(transNum);
                // Handle case when transaction is not in transactionTable
                if (!transactionTable.containsKey(transNum)) {
                    startTransaction(currTransaction);
                }
                TransactionTableEntry entry = transactionTable.get(transNum);
                // Update lastLSN
                setLastLSN(transNum, LR.LSN);
                if (LR.getPageNum().isPresent()) {
                    //if (pageLogTypes.contains(LRType) && LR.getPageNum().isPresent()) {
                    // Case 1.1: Log Record involves a Page
                    Long pageNumber = LR.getPageNum().get();
                    // Add page to touchedPages set
                    addTouchedPage(transNum, pageNumber);
                    // Transaction must request X Lock on the page
                    acquireTransactionLock(entry.transaction, getPageLockContext(pageNumber), LockType.X);
                    // Case 1.1.1: Log Record involves Update_Page/Undo_Update_Page
                    if (LRType == LogType.UPDATE_PAGE || LRType == LogType.UNDO_UPDATE_PAGE) {
                        addToDirtyTable(transNum, pageNumber);
                    }
                    // Case 1.1.2: Log Record involves Alloc_Page/Free_Page/Undo_Alloc_Page/Undo_Free_Page
                    // Because these log types make their changes to the page visible on disk immediately,
                    // we can remove this page from the dirty table
                    else {
                        removeFromDirtyTable(pageNumber);
                    }
                }

                handleStatusUpdates(LR, entry);
            }

            // Case 3: Log Record involves a Checkpoint
            // If BeginCheckPoint, update transaction counter
            if (LRType == LogType.BEGIN_CHECKPOINT) {
                // updateTransactionCounter is a Consumer Interface type
                // Set counter to the max of current value and this LogRecord's max transaction number
                Long arg = LR.getMaxTransactionNum().isPresent() ?
                            Math.max(LR.getMaxTransactionNum().get(), getTransactionCounter.get())
                            : getTransactionCounter.get();
                updateTransactionCounter.accept(arg);
                }
            // If LRType is EndCheckpoint
            else if (LRType == LogType.END_CHECKPOINT) {
                // When an EndCheckpoint record is encountered, the tables stored in the record
                // should be combined with the tables currently in memory.

                // Combine DPT tables
                for (Map.Entry<Long, Long> dirtyEntry : LR.getDirtyPageTable().entrySet()) {
                    Long pageNum = dirtyEntry.getKey();
                    Long recLSN = dirtyEntry.getValue();
                    // recLSN of a page should always be used since the CheckPoint is likely more accurate
                    dirtyPageTable.put(pageNum, recLSN);
                }

                // Combine Xact tables
                // See LogRecord.java for type of LogRecord.getTransactionTable().entrySet()
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> transactionEntry : LR.getTransactionTable().entrySet()) {
                    Long entryTransNum = transactionEntry.getKey();
                    Long entryLSN = transactionEntry.getValue().getSecond();
                    Transaction.Status entryStatus = transactionEntry.getValue().getFirst();
                    TransactionTableEntry inTable = transactionTable.get(entryTransNum);

                    if (entryStatus == Transaction.Status.ABORTING) {
                        inTable.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                    }

                    if (inTable != null) {
                        inTable.lastLSN = Math.max(inTable.lastLSN, entryLSN);
                    }
                    else {
                        //System.out.println("Entries not in memory, but in log: ");
                        startTransaction(newTransaction.apply(entryTransNum));
                        //System.out.println(transactionTable.get(entryTransNum));
                        setLastLSN(entryTransNum, entryLSN);
                    }
                }

                // Combine touchedPages tables
                for (Map.Entry<Long, List<Long>> touchedEntry : LR.getTransactionTouchedPages().entrySet()) {
                    Long entryTransNum = touchedEntry.getKey();
                    List<Long> touchedPages = touchedEntry.getValue();
                    if (transactionTable.get(entryTransNum) != null) {
                        TransactionTableEntry entry = transactionTable.get(entryTransNum);
                        if (entry.transaction.getStatus() != Transaction.Status.COMPLETE) {
                            for (long page : touchedPages) {
                                HashSet<Long> tableTouchedPages = new HashSet<>(entry.touchedPages);
                                if (!tableTouchedPages.contains(page)) {
                                    tableTouchedPages.add(page);
                                    acquireTransactionLock(entry.transaction, getPageLockContext(page), LockType.X);
                                }
                            }
                        }
                    }
                }
            }
        }

        /////// Ending Transactions ////////
        for (Map.Entry<Long, TransactionTableEntry> transactionEntry : transactionTable.entrySet()) {
            TransactionTableEntry value = transactionEntry.getValue();
            Long transNum = transactionEntry.getKey();
            Transaction.Status status = value.transaction.getStatus();
            if (status == Transaction.Status.COMMITTING) {
                value.transaction.cleanup();
                end(value.transaction.getTransNum());
            } else if (status == Transaction.Status.RUNNING) {
                abort(value.transaction.getTransNum());
                // Abort() needs to be called first so that we can correctly set the state of the transaction
                TransactionTableEntry e = transactionTable.get(value.transaction.getTransNum());
                e.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
        }
    }
/**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */

    void restartRedo() {
        HashSet<LogType> pageLogTypes = getPageLogTypes();
        HashSet<LogType> partititionLogTypes = getPartitionLogTypes();

        // Redo phase begins at the lowest recLSN in the dirty page table
        Iterator<LogRecord> LRIterator = logManager.scanFrom(Collections.min(dirtyPageTable.values()));

        while (LRIterator.hasNext()) {
            LogRecord LR = LRIterator.next();
            LogType LRType = LR.type;

            if (LR.isRedoable()) {
                // Partition-related record
                // No special requirements for partitionLogType LR lol
                if (partititionLogTypes.contains(LRType)) {
                    LR.redo(diskSpaceManager, bufferManager);
                }
                // Page-related record
                else if (pageLogTypes.contains(LRType)) {
                    if (LR.getPageNum().isPresent()) {
                        Long pageNum = LR.getPageNum().get();
                        if (dirtyPageTable.containsKey(pageNum)) {
                            // Log Record LSN must not be less than recLSN
                            if (LR.LSN >= dirtyPageTable.get(pageNum)) {
                                long pageLSN = bufferManager.fetchPage(getPageLockContext(pageNum).parentContext(), pageNum, false).getPageLSN();
                                // pageLSN must be less than the Log Record's LSN
                                if (pageLSN < LR.LSN) {
                                    LR.redo(diskSpaceManager, bufferManager);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
/**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */

    void restartUndo() {
        // Construct maxHeap PQ over lastLSNs
        PriorityQueue<Long> abortingTransactions = new PriorityQueue<>(Collections.reverseOrder());
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            if (entry.getValue().transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                abortingTransactions.add(entry.getValue().lastLSN);
            }
        }

        while (abortingTransactions.size() != 0) {
            Long LSN = abortingTransactions.poll();
            LogRecord LR = fetchLogRecord(LSN);
            Long transNum = LR.getTransNum().get();
            if (LR.isUndoable()) {
                // IMPORTANT: CLR must be emitted BEFORE flush, table update and redo
                // Undo record
                Pair<LogRecord, Boolean> CLR = LR.undo(transactionTable.get(LR.getTransNum().get()).lastLSN);
                LogRecord record = CLR.getFirst();
                // Emit CLR
                logManager.appendToLog(record);
                logManager.flushToLSN(record.LSN);
                // Update table
                setLastLSN(record.getTransNum().get(), record.LSN);
                record.redo(diskSpaceManager, bufferManager);
            }

            boolean removeFlag = false;
            if (LR.getUndoNextLSN().isPresent()) {
                // Replace this entry with that of UndoNextLSN in the set if it's not 0
                if (LR.getUndoNextLSN().get() != 0) {
                    abortingTransactions.add(LR.getUndoNextLSN().get());
                } else {
                    removeFlag = true;
                }
            }
            else if (LR.getPrevLSN().isPresent()) {
                // Replace this entry with that of getPrevLSN in the set if it's not 0
                if (LR.getPrevLSN().get() != 0) {
                    abortingTransactions.add(LR.getPrevLSN().get());
                } else {
                    removeFlag = true;
                }
            }
            if (removeFlag) {
                logManager.appendToLog(new EndTransactionLogRecord(transNum, LSN));
                transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(transNum);
            }
        }
    }

    // Add allllll the helper methods

    public HashSet<LogType> getTransactionLogTypes() {
        HashSet<LogType> transactionLogTypes = new HashSet<>(Arrays.asList(LogType.values()));
        transactionLogTypes.remove(LogType.MASTER);
        transactionLogTypes.remove(LogType.COMMIT_TRANSACTION);
        transactionLogTypes.remove(LogType.ABORT_TRANSACTION);
        transactionLogTypes.remove(LogType.END_TRANSACTION);
        transactionLogTypes.remove(LogType.BEGIN_CHECKPOINT);
        transactionLogTypes.remove(LogType.END_CHECKPOINT);
        return transactionLogTypes;
    }

    public HashSet<LogType> getPageLogTypes() {
        HashSet<LogType> pageLogTypes = new HashSet<>();
        pageLogTypes.add(LogType.ALLOC_PAGE);
        pageLogTypes.add(LogType.UPDATE_PAGE);
        pageLogTypes.add(LogType.FREE_PAGE);
        pageLogTypes.add(LogType.UNDO_ALLOC_PAGE);
        pageLogTypes.add(LogType.UNDO_UPDATE_PAGE);
        pageLogTypes.add(LogType.UNDO_FREE_PAGE);
        return pageLogTypes;
    }

    public HashSet<LogType> getStatusLogTypes() {
        HashSet<LogType> statusLogTypes = new HashSet<>();
        statusLogTypes.add(LogType.COMMIT_TRANSACTION);
        statusLogTypes.add(LogType.ABORT_TRANSACTION);
        statusLogTypes.add(LogType.END_TRANSACTION);
        return statusLogTypes;
    }

    public HashSet<LogType> getCheckpointLogTypes() {
        HashSet<LogType> checkpointLogTypes = new HashSet<>();
        checkpointLogTypes.add(LogType.BEGIN_CHECKPOINT);
        checkpointLogTypes.add(LogType.END_CHECKPOINT);
        return checkpointLogTypes;
    }

    public HashSet<LogType> getPartitionLogTypes() {
        HashSet<LogType> partitionLogTypes = new HashSet<>();
        partitionLogTypes.add(LogType.ALLOC_PART);
        partitionLogTypes.add(LogType.FREE_PART);
        partitionLogTypes.add(LogType.UNDO_ALLOC_PART);
        partitionLogTypes.add(LogType.UNDO_FREE_PART);
        return partitionLogTypes;
    }

    public void handleStatusUpdates(LogRecord LR, TransactionTableEntry entry) {
        Long transNum = entry.transaction.getTransNum();
        if (LR.type == LogType.COMMIT_TRANSACTION) {
            entry.transaction.setStatus(Transaction.Status.COMMITTING);
        } else if (LR.type == LogType.ABORT_TRANSACTION) {
            entry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        } else if (LR.type == LogType.END_TRANSACTION) {
            entry.transaction.cleanup();
            entry.transaction.setStatus(Transaction.Status.COMPLETE);
            transactionTable.remove(transNum);
        }
    }


    // Helpers ///////////////////////////////////////////////////////////////////////////////


/**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */

    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }


/**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */

    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }


/**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */

    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                        lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }


/**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */

    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
