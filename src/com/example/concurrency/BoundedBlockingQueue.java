package com.example.concurrency;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

class BoundedBlockingQueue<T> {
    private final Object[] items;                 // circular buffer
    private int head, tail, count;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    BoundedBlockingQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        items = new Object[capacity];
    }

    public void put(T item) throws InterruptedException {
        Objects.requireNonNull(item);
        lock.lockInterruptibly();
        try {
            while (count == items.length) notFull.await();  // WHILE: spurious wakeups + races
            items[tail] = item;
            tail = (tail + 1) % items.length;
            count++;
            notEmpty.signal();                               // wake one waiting consumer
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    public T take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (count == 0) notEmpty.await();
            return dequeue();
        } finally {
            lock.unlock();
        }
    }

    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (count == 0) {
                if (nanos <= 0) return null;
                nanos = notEmpty.awaitNanos(nanos);   // returns remaining time
            }
            return dequeue();
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private T dequeue() {                          // caller holds the lock
        T item = (T) items[head];
        items[head] = null;                        // let GC reclaim it
        head = (head + 1) % items.length;
        count--;
        notFull.signal();                          // wake one waiting producer
        return item;
    }


}
