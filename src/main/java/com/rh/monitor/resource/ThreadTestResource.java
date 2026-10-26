package com.rh.monitor.resource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;

@Path("/api/thread-test")
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class ThreadTestResource {

    private static final Logger LOG = Logger.getLogger(ThreadTestResource.class);

    private static final Object SHARED_LOCK = new Object();
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final List<Thread> activeThreads = new CopyOnWriteArrayList<>();

    @POST
    @Path("/blocked")
    public Map<String, Object> createBlocked(
            @QueryParam("threads") @DefaultValue("5") int threadCount,
            @QueryParam("hold") @DefaultValue("60") int holdSeconds) {

        LOG.infof("Creating blocked thread scenario: threads=%d, hold=%ds", threadCount, holdSeconds);
        int id = counter.incrementAndGet();
        Object lock = new Object();

        Thread holder = new Thread(() -> {
            synchronized (lock) {
                try { Thread.sleep(holdSeconds * 1000L); } catch (InterruptedException ignored) {}
            }
        }, "ThreadTest-LockHolder-" + id);
        holder.setDaemon(true);
        holder.start();
        activeThreads.add(holder);

        try { Thread.sleep(100); } catch (InterruptedException ignored) {}

        for (int i = 0; i < threadCount; i++) {
            Thread blocked = new Thread(() -> {
                synchronized (lock) {
                    try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
                }
            }, "ThreadTest-Blocked-" + id + "-" + i);
            blocked.setDaemon(true);
            blocked.start();
            activeThreads.add(blocked);
        }

        return Map.of("status", "ok",
                "scenario", "blocked",
                "threads_created", threadCount + 1,
                "hold_seconds", holdSeconds,
                "message", threadCount + " threads BLOCKED waiting for lock held " + holdSeconds + "s");
    }

    @POST
    @Path("/deadlock")
    public Map<String, Object> createDeadlock() {
        LOG.info("Creating deadlock test scenario");
        int id = counter.incrementAndGet();
        Object lockX = new Object();
        Object lockY = new Object();
        CountDownLatch bothLocked = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            synchronized (lockX) {
                bothLocked.countDown();
                try { bothLocked.await(); } catch (InterruptedException ignored) {}
                synchronized (lockY) { /* never reached — deadlocked */ }
            }
        }, "ThreadTest-Deadlock-A-" + id);

        Thread t2 = new Thread(() -> {
            synchronized (lockY) {
                bothLocked.countDown();
                try { bothLocked.await(); } catch (InterruptedException ignored) {}
                synchronized (lockX) { /* never reached — deadlocked */ }
            }
        }, "ThreadTest-Deadlock-B-" + id);

        t1.setDaemon(true);
        t2.setDaemon(true);
        t1.start();
        t2.start();
        activeThreads.add(t1);
        activeThreads.add(t2);

        // Wait for both threads to hold their first lock and then attempt the second
        try {
            bothLocked.await(5, TimeUnit.SECONDS);
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {}

        // Verify deadlock via JVM ThreadMXBean (monitor deadlocks only)
        boolean verified = false;
        int retries = 3;
        while (!verified && retries-- > 0) {
            try {
                ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
                long[] deadlockedIds = tmx.findMonitorDeadlockedThreads();
                if (deadlockedIds != null) {
                    for (long did : deadlockedIds) {
                        if (did == t1.getId() || did == t2.getId()) {
                            verified = true;
                            break;
                        }
                    }
                }
                if (!verified) {
                    Thread.sleep(500);
                }
            } catch (Exception ignored) {}
        }

        String nameA = "ThreadTest-Deadlock-A-" + id;
        String nameB = "ThreadTest-Deadlock-B-" + id;
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "ok");
        result.put("scenario", "deadlock");
        result.put("threads_created", 2);
        result.put("verified", verified);
        result.put("t1_state", t1.getState().name());
        result.put("t2_state", t2.getState().name());
        result.put("message", "Deadlock " + (verified ? "VERIFIED" : "created (unverified)")
                + ": " + nameA + " <-> " + nameB);
        return result;
    }

    @POST
    @Path("/long-running")
    public Map<String, Object> createLongRunning(
            @QueryParam("threads") @DefaultValue("3") int threadCount,
            @QueryParam("duration") @DefaultValue("120") int durationSeconds) {

        int id = counter.incrementAndGet();
        for (int i = 0; i < threadCount; i++) {
            int idx = i;
            Thread t = new Thread(() -> {
                long end = System.currentTimeMillis() + durationSeconds * 1000L;
                long sum = 0;
                while (System.currentTimeMillis() < end && !Thread.currentThread().isInterrupted()) {
                    for (int j = 0; j < 500_000; j++) sum += j * idx;
                }
            }, "ThreadTest-LongRunning-" + id + "-" + i);
            t.setDaemon(true);
            t.start();
            activeThreads.add(t);
        }

        return Map.of("status", "ok",
                "scenario", "long-running",
                "threads_created", threadCount,
                "duration_seconds", durationSeconds,
                "message", threadCount + " RUNNABLE threads doing CPU work for " + durationSeconds + "s");
    }

    @POST
    @Path("/contention")
    public Map<String, Object> createContention(
            @QueryParam("threads") @DefaultValue("8") int threadCount,
            @QueryParam("duration") @DefaultValue("60") int durationSeconds) {

        int id = counter.incrementAndGet();
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                long end = System.currentTimeMillis() + durationSeconds * 1000L;
                while (System.currentTimeMillis() < end && !Thread.currentThread().isInterrupted()) {
                    synchronized (SHARED_LOCK) {
                        try { Thread.sleep(200 + (long)(Math.random() * 300)); } catch (InterruptedException ignored) { break; }
                    }
                    try { Thread.sleep(50); } catch (InterruptedException ignored) { break; }
                }
            }, "ThreadTest-Contention-" + id + "-" + i);
            t.setDaemon(true);
            t.start();
            activeThreads.add(t);
        }

        return Map.of("status", "ok",
                "scenario", "contention",
                "threads_created", threadCount,
                "duration_seconds", durationSeconds,
                "message", threadCount + " threads contending on shared lock for " + durationSeconds + "s");
    }

    @POST
    @Path("/waiting")
    public Map<String, Object> createWaiting(
            @QueryParam("threads") @DefaultValue("5") int threadCount,
            @QueryParam("duration") @DefaultValue("120") int durationSeconds) {

        int id = counter.incrementAndGet();
        Object waitLock = new Object();
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                synchronized (waitLock) {
                    try { waitLock.wait(durationSeconds * 1000L); } catch (InterruptedException ignored) {}
                }
            }, "ThreadTest-Waiting-" + id + "-" + i);
            t.setDaemon(true);
            t.start();
            activeThreads.add(t);
        }

        return Map.of("status", "ok",
                "scenario", "waiting",
                "threads_created", threadCount,
                "duration_seconds", durationSeconds,
                "message", threadCount + " threads in TIMED_WAITING for " + durationSeconds + "s");
    }

    @POST
    @Path("/all")
    public Map<String, Object> createAll() {
        LOG.info("Creating all thread test scenarios");
        List<String> scenarios = new ArrayList<>();
        int total = 0;

        // Blocked
        var b = createBlocked(3, 90);
        total += (int) b.get("threads_created");
        scenarios.add((String) b.get("message"));

        // Deadlock
        var d = createDeadlock();
        total += (int) d.get("threads_created");
        scenarios.add((String) d.get("message"));

        // Long-running
        var lr = createLongRunning(2, 90);
        total += (int) lr.get("threads_created");
        scenarios.add((String) lr.get("message"));

        // Contention
        var c = createContention(4, 90);
        total += (int) c.get("threads_created");
        scenarios.add((String) c.get("message"));

        // Waiting
        var w = createWaiting(3, 90);
        total += (int) w.get("threads_created");
        scenarios.add((String) w.get("message"));

        return Map.of("status", "ok",
                "scenario", "all",
                "threads_created", total,
                "scenarios", scenarios);
    }

    @GET
    @Path("/status")
    public Map<String, Object> getStatus() {
        activeThreads.removeIf(t -> !t.isAlive());
        List<Map<String, Object>> threads = new ArrayList<>();
        for (Thread t : activeThreads) {
            threads.add(Map.of(
                    "name", t.getName(),
                    "state", t.getState().name(),
                    "alive", t.isAlive()));
        }
        Map<String, Integer> stateCounts = new LinkedHashMap<>();
        for (Thread t : activeThreads) {
            stateCounts.merge(t.getState().name(), 1, Integer::sum);
        }
        return Map.of("status", "ok",
                "active_count", activeThreads.size(),
                "state_summary", stateCounts,
                "threads", threads);
    }

    @GET
    @Path("/deadlock-check")
    public Map<String, Object> checkDeadlocks() {
        LOG.info("Checking for deadlocks in local JVM");
        ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
        long[] dlIds = tmx.findMonitorDeadlockedThreads();
        List<Map<String, String>> details = new ArrayList<>();
        if (dlIds != null) {
            java.lang.management.ThreadInfo[] infos = tmx.getThreadInfo(dlIds, true, true);
            for (int i = 0; i < dlIds.length; i++) {
                java.lang.management.ThreadInfo ti = infos[i];
                if (ti == null) continue;
                String name = ti.getThreadName();
                if (isInfraThread(name)) continue;
                Map<String, String> info = new LinkedHashMap<>();
                info.put("thread_id", String.valueOf(dlIds[i]));
                info.put("thread_name", name);
                info.put("state", ti.getThreadState().name());
                info.put("lock_name", ti.getLockName() != null ? ti.getLockName() : "");
                info.put("lock_owner", ti.getLockOwnerName() != null ? ti.getLockOwnerName() : "");
                details.add(info);
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("deadlock_count", details.size());
        result.put("threads", details);
        return result;
    }

    private static boolean isInfraThread(String name) {
        if (name == null) return false;
        return name.startsWith("management I/O-")
                || name.startsWith("default I/O-")
                || name.startsWith("XNIO-")
                || name.startsWith("executor-thread-")
                || name.startsWith("vert.x-");
    }

    @POST
    @Path("/cleanup")
    public Map<String, Object> cleanup() {
        int count = activeThreads.size();
        for (Thread t : activeThreads) t.interrupt();
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        activeThreads.removeIf(t -> !t.isAlive());
        int remaining = activeThreads.size();
        return Map.of("status", "ok",
                "interrupted", count,
                "remaining", remaining,
                "message", "Interrupted " + count + " threads. " + remaining + " still alive (deadlocked threads cannot be interrupted).");
    }
}
