package com.example.concurrency;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

public class TtlLruCache<K, V> implements AutoCloseable {
    private static final class Node<K, V> {
        final K key;
        V value;
        long expiresAtNanos;
        Node<K, V> prev, next;

        Node(K key, V value, long exp) {
            this.key = key;
            this.value = value;
            this.expiresAtNanos = exp;
        }
    }

    private final int capacity;
    private final LongSupplier clock;              // System::nanoTime in prod; a fake clock in tests
    private final Map<K, Node<K, V>> map = new HashMap<>();
    private final Node<K, V> head = new Node<>(null, null, 0);
    private final Node<K, V> tail = new Node<>(null, null, 0);
    private final ReentrantLock lock = new ReentrantLock();
    private final ScheduledExecutorService sweeper;
    private final AtomicLong hits = new AtomicLong(), misses = new AtomicLong(), evictions = new AtomicLong();

    public TtlLruCache(int capacity, LongSupplier clock, long sweepEveryMs) {
        this.capacity = capacity;
        this.clock = clock;
        head.next = tail;
        tail.prev = head;
        sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-sweeper");
            t.setDaemon(true);                     // never blocks JVM exit
            return t;
        });
        sweeper.scheduleAtFixedRate(this::sweepExpired, sweepEveryMs, sweepEveryMs, TimeUnit.MILLISECONDS);
    }

    public V get(K key) {
        lock.lock();
        try {
            Node<K, V> n = map.get(key);
            if (n == null) {
                misses.incrementAndGet();
                return null;
            }
            if (isExpired(n)) {                    // lazy expiry: never serve stale data
                remove(n);
                misses.incrementAndGet();
                return null;
            }
            moveToFront(n);
            hits.incrementAndGet();
            return n.value;
        } finally {
            lock.unlock();
        }
    }

    public void put(K key, V value, long ttl, TimeUnit unit) {
        long expiresAt = clock.getAsLong() + unit.toNanos(ttl);
        lock.lock();
        try {
            Node<K, V> n = map.get(key);
            if (n != null) {
                n.value = value;
                n.expiresAtNanos = expiresAt;
                moveToFront(n);
                return;
            }
            if (map.size() == capacity) evictOne();
            n = new Node<>(key, value, expiresAt);
            map.put(key, n);
            addFirst(n);
        } finally {
            lock.unlock();
        }
    }

    private void evictOne() {                      // prefer an expired entry, else the LRU one
        for (Node<K, V> n = tail.prev; n != head; n = n.prev) {
            if (isExpired(n)) {
                remove(n);
                evictions.incrementAndGet();
                return;
            }
        }
        remove(tail.prev);
        evictions.incrementAndGet();
    }

    void sweepExpired() {                          // background cleanup of entries nobody reads
        lock.lock();
        try {
            Node<K, V> n = tail.prev;
            while (n != head) {
                Node<K, V> prev = n.prev;          // save before unlink clears pointers
                if (isExpired(n)) remove(n);
                n = prev;
            }
        } finally {
            lock.unlock();
        }
    }

    public String stats() {
        return "hits=" + hits + " misses=" + misses + " evictions=" + evictions;
    }

    @Override
    public void close() {
        sweeper.shutdownNow();
    }

    private boolean isExpired(Node<K, V> n) {
        return clock.getAsLong() - n.expiresAtNanos >= 0;
    } // overflow-safe

    private void remove(Node<K, V> n) {
        unlink(n);
        map.remove(n.key);
    }

    private void moveToFront(Node<K, V> n) {
        unlink(n);
        addFirst(n);
    }

    private void addFirst(Node<K, V> n) {
        n.next = head.next;
        n.prev = head;
        head.next.prev = n;
        head.next = n;
    }

    private void unlink(Node<K, V> n) {
        n.prev.next = n.next;
        n.next.prev = n.prev;
        n.prev = n.next = null;
    }
}
