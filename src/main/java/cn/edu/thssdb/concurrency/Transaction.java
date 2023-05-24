package cn.edu.thssdb.concurrency;
import cn.edu.thssdb.utils.RID;

import java.util.HashMap;
import java.util.HashSet;


public class Transaction {
    private int txn_id;
    private long thread_id;
    private TransactionState state = TransactionState.GROWING;
    private IsolationLevel isolationLevel;
    private HashSet<String> s_table_lock_set;
    private HashSet<String> x_table_lock_set;
    private HashMap<String, HashSet<RID>> s_row_lock_set;
    private HashMap<String, HashSet<RID>> x_row_lock_set;

    public Transaction(int txn_id, IsolationLevel iso_lvl){
        this.txn_id = txn_id;
        this.isolationLevel = iso_lvl;
        this.thread_id = Thread.currentThread().getId();
        s_table_lock_set = new HashSet<>();
        x_table_lock_set = new HashSet<>();
        s_row_lock_set = new HashMap<>();
        x_row_lock_set = new HashMap<>();
    }

    public TransactionState getState() {
        return state;
    }

    public int getTxn_id() {
        return txn_id;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public boolean IsTableSharedLocked(String tableName) {
        return s_table_lock_set.contains(tableName);
    }

    public boolean IsTableExclusiveLocked(String tableName) {
        return x_table_lock_set.contains(tableName);
    }

    public boolean IsRowSharedLocked(String tableName, RID rid) {
        HashSet<RID> ridSet = s_row_lock_set.get(tableName);
        if (ridSet == null) {
            return false;
        }
        else {
            return ridSet.contains(rid);
        }
    }

    public boolean IsRowExclusiveLocked(String tableName, RID rid) {
        HashSet<RID> ridSet = x_row_lock_set.get(tableName);
        if (ridSet == null) {
            return false;
        }
        else {
            return ridSet.contains(rid);
        }
    }
}

