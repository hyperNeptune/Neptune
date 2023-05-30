package cn.edu.thssdb.concurrency;

enum IsolationLevel {
    READ_COMMITTED,
    SERIALIZED,
    READ_UNCOMMITTED,
    REPEATABLE_READ
}
