package com.example.concurrency;

import java.util.concurrent.*;
import java.util.function.Function;

public class OnceCache<K, V> {

    private final ConcurrentHashMap<K, CompletableFuture<V>> cache = new ConcurrentHashMap<>();
    private final Function<K, V> loader;

    public OnceCache(Function<K, V> loader) {
        this.loader = loader;
    }

    public V get(K key) {
        CompletableFuture<V> created = new CompletableFuture<>();
        CompletableFuture<V> existing = cache.putIfAbsent(key, created); // cheap, atomic
        if (existing == null) {                    // we won: load OUTSIDE any map lock
            try {
                created.complete(loader.apply(key));
            } catch (Throwable t) {
                cache.remove(key, created);        // don't cache failures; next call retries
                created.completeExceptionally(t);
            }
            existing = created;
        }
        try {
            return existing.join();                // losers wait for the winner's result
        } catch (CompletionException e) {
            throw new RuntimeException("load failed for " + key, e.getCause());
        }
    }

    static void main(String[] args) throws InterruptedException {
        // Define a loader that simulates an expensive database/API fetch (takes 500ms)
        Function<String, String> dbLoader = key -> {
            System.out.println("   --> [LOADER] Fetching from database for key: " + key);
            try {
                Thread.sleep(500); // simulate network/DB delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "Data-for-" + key.toUpperCase();
        };

        OnceCache<String, String> cache = new OnceCache<>(dbLoader);

        System.out.println("=== TEST 1: Multiple threads requesting the SAME key simultaneously ===");
        int threadCount = 5;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        // Fire 5 threads asking for "product-123" at the exact same time
        for (int i = 1; i <= threadCount; i++) {
            final int threadId = i;
            pool.submit(() -> {
                long start = System.currentTimeMillis();
                String result = cache.get("product-123");
                long duration = System.currentTimeMillis() - start;
                System.out.println("Thread " + threadId + " finished. Result: " + result + " (Took " + duration + "ms)");
            });
        }

        // Wait for Test 1 threads to finish
        Thread.sleep(1500);

        System.out.println("\n=== TEST 2: Requesting an ALREADY cached key ===");
        long start = System.currentTimeMillis();
        String cachedResult = cache.get("product-123");
        long duration = System.currentTimeMillis() - start;
        System.out.println("Instant lookup Result: " + cachedResult + " (Took " + duration + "ms)");

        pool.shutdown();
    }
}
