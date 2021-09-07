package com.spotify.dns;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.xbill.DNS.*;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DnsLookupPerformanceTest {
    private static AtomicInteger successCount = new AtomicInteger(0);

    private static DnsSrvResolver resolver = DnsSrvResolvers.newBuilder()
            .cachingLookups(false)
            .retainingDataOnFailures(false)
            .dnsLookupTimeoutMillis(5000)
            .build();

    @Test
    public void runTest() throws InterruptedException {
        long startTime = System.nanoTime();
        int numThreads = 3;
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<String> records = List.of(
                "_spotify-noop._http.services.gew1.spotify.net.",
                "_spotify-noop._http.services.guc3.spotify.net.",
                "_spotify-noop._http.services.gae2.spotify.net.",
                "_spotify-palindrome._grpc.services.gae2.spotify.net.",
                "_spotify-palindrome._grpc.services.gew1.spotify.net.",
                "_spotify-concat._grpc.services.gew1.spotify.net.",
                "_spotify-concat._grpc.services.guc3.spotify.net.",
                "_spotify-concat._hm.services.gae2.spotify.net.",
                "_spotify-concat._hm.services.gew1.spotify.net.",
                "_spotify-concat._hm.services.guc3.spotify.net.",
                "_spotify-fabric-test._grpc.services.gae2.spotify.net.",
                "_spotify-fabric-test._grpc.services.gew1.spotify.net.",
                "_spotify-fabric-test._grpc.services.guc3.spotify.net.",
                "_spotify-fabric-test._hm.services.gae2.spotify.net.",
                "_spotify-fabric-test._hm.services.gew1.spotify.net.",
                "_spotify-fabric-test._hm.services.guc3.spotify.net.",
                "_spotify-fabric-load-generator._grpc.services.gae2.spotify.net.",
                "_spotify-fabric-load-generator._grpc.services.gew1.spotify.net.",
                "_spotify-fabric-load-generator._grpc.services.guc3.spotify.net.",
                "_spotify-client._tcp.spotify.com");

        CountDownLatch done = new CountDownLatch(records.size() * 2);
        records.stream()
                .forEach(
                        fqdn -> {
                            executorService.submit(() -> resolve(fqdn, done));
                            CompletableFuture.runAsync(DnsLookupPerformanceTest::blockCommonPool)
                                    .whenComplete((v, ex) -> done.countDown());
                        });
        done.await(1, TimeUnit.MINUTES);
        executorService.shutdown();

        int failureCount = records.size() - successCount.get();
        long duration = System.nanoTime() - startTime;

        System.out.println("Number of threads: " + numThreads);
        System.out.println("Number of records: " + records.size());
        System.out.println("Failed lookups: " + failureCount);
        System.out.println("Duration ms: " + duration/1000/1000);

        assertThat(failureCount, equalTo(0));
    }

    private static void blockCommonPool() {
        try {
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void resolve(String fqdn, CountDownLatch done) {
        try {
            System.out.println("Resolving: " + fqdn);
            List<LookupResult> results = resolver.resolveAsync(fqdn).toCompletableFuture().get();

            if(!results.isEmpty()) {
                successCount.incrementAndGet();
                System.out.println(fqdn + "...ok!");
            } else {
                System.err.format("%s ... failed!\n", fqdn);
            }
        } catch (Exception e) {
            System.err.format("%s ... failed!\n", fqdn);
            e.printStackTrace(System.err);
        } finally {
            done.countDown();
        }
    }
}