package cn.edu.thssdb.concurrency;

import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrencyTest {
    private class TestThread implements Runnable {
        private final Transaction txn;
        private final String tableName;
        private final LockManager lockManager;
        private final LockManager.LockMode mode;
        public TestThread(Transaction txn,
                          String name, LockManager lm,
                          LockManager.LockMode mode){
            this.txn = txn;
            this.tableName = name;
            this.lockManager = lm;
            this.mode = mode;
        }
        private ReentrantLock tlock = new ReentrantLock();

        @Override
        public void run() {
            for (int i = 0; i < 10; i ++)
            {
                //tlock.lock();
                if (lockManager.lockTable(txn, mode, tableName))
                    System.out.println(txn.getTxn_id() + ": Get " + mode + " lock");
                //tlock.unlock();

                //tlock.lock();
                if(lockManager.unlockTable(txn, tableName))
                    System.out.println(txn.getTxn_id() + ": Release lock");
                //tlock.unlock();
            }
        }
    }

    @Test
    public void tableLockTest() {
        Transaction a = new Transaction(0, IsolationLevel.READ_COMMITTED);
        Transaction b = new Transaction(1, IsolationLevel.READ_COMMITTED);
        Transaction c = new Transaction(2, IsolationLevel.READ_COMMITTED);

        String table = "table";
        LockManager lm = new LockManager();
        Thread thread1 = new Thread(new TestThread(a, table, lm, LockManager.LockMode.SHARED));
        Thread thread2 = new Thread(new TestThread(b, table, lm, LockManager.LockMode.SHARED));
        Thread thread3 = new Thread(new TestThread(c, table, lm, LockManager.LockMode.EXCLUSIVE));

        thread1.start();
        thread2.start();
        thread3.start();

        try {
            thread1.join();
            thread2.join();
            thread3.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
