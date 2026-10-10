package com.example.concurrency;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class LruCache<K, V> {
    private static final class Node<K, V> {
        final K key;
        V value;
        Node<K, V> prev, next;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private final int capacity;
    private final Map<K, Node<K, V>> map = new HashMap<>();
    private final Node<K, V> head = new Node<>(null, null);  // sentinel; most recent is head.next
    private final Node<K, V> tail = new Node<>(null, null);  // sentinel; least recent is tail.prev
    private final ReentrantLock lock = new ReentrantLock();

    public LruCache(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity = capacity;
        head.next = tail;
        tail.prev = head;
    }

    public V get(K key) {
        lock.lock();
        try {
            Node<K, V> n = map.get(key);
            if (n == null) return null;
            moveToFront(n);              // a read CHANGES the order -> a read lock is not enough
            return n.value;
        } finally {
            lock.unlock();
        }
    }

    public void put(K key, V value) {
        lock.lock();
        try {
            Node<K, V> n = map.get(key);
            if (n != null) {             // update existing
                n.value = value;
                moveToFront(n);
                return;
            }
            if (map.size() == capacity) { // evict least recently used
                Node<K, V> lru = tail.prev;
                unlink(lru);
                map.remove(lru.key);     // this is why the node stores its key
            }
            n = new Node<>(key, value);
            map.put(key, n);
            addFirst(n);
        } finally {
            lock.unlock();
        }
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
