package com.example.concurrency;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

class EventCounterAtomic {
    private final ConcurrentHashMap<String, LongAdder> counts = new ConcurrentHashMap<>();

    void record(String key) {
        counts.computeIfAbsent(key, k -> new LongAdder()).increment(); // atomic create + lock-free add
    }

    long get(String key) {
        LongAdder a = counts.get(key);
        return a == null ? 0 : a.sum();
    }

    List<Map.Entry<String, Long>> topN(int n) {
        List<Map.Entry<String, Long>> all = new ArrayList<>();
        counts.forEach((k, v) -> all.add(Map.entry(k, v.sum())));   // weakly consistent snapshot
        all.sort(Map.Entry.<String, Long> comparingByValue().reversed());
        return all.subList(0, Math.min(n, all.size()));
    }
}
