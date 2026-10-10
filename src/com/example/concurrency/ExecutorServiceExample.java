package com.example.concurrency;

import java.util.*;
import java.util.concurrent.*;

public class ExecutorServiceExample {

    private final ExecutorService pool;          // injected: shared, configurable, testable

    ExecutorServiceExample(ExecutorService pool) {
        this.pool = pool;
    }

    static int fetchPrice(String productId) throws InterruptedException {
        Thread.sleep(100);                       // simulated network call
        if (productId.equals("bad")) throw new IllegalStateException("upstream 500");
        return productId.length() * 10;
    }

    Map<String, Integer> fetchAll(List<String> ids, long timeoutMs) throws InterruptedException {
        List<Callable<Integer>> tasks = new ArrayList<>();
        for (String id : ids) tasks.add(() -> fetchPrice(id));

        // Blocks until all finish or timeout; unfinished tasks are cancelled
        List<Future<Integer>> futures = pool.invokeAll(tasks, timeoutMs, TimeUnit.MILLISECONDS);

        Map<String, Integer> result = new LinkedHashMap<>();
        for (int i = 0; i < ids.size(); i++) {
            try {
                result.put(ids.get(i), futures.get(i).get());  // done already, no blocking
            } catch (CancellationException e) {
                result.put(ids.get(i), -1);                     // timed out
            } catch (ExecutionException e) {
                result.put(ids.get(i), -2);                     // task threw; cause = e.getCause()
            }
        }
        return result;
    }
}

