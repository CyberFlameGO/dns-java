package com.spotify.dns;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class DnsLookupPerformanceTest {
    private static AtomicInteger successCount = new AtomicInteger(0);

    private static DnsSrvResolver resolver = DnsSrvResolvers.newBuilder()
            .cachingLookups(false)
            .retainingDataOnFailures(true)
            .dnsLookupTimeoutMillis(5000)
            .build();

    //@Test
    // TODO: make this test pass
    public void runTest() throws InterruptedException {
        int numThreads = 50;
        final ExecutorService smallExecutorService = Executors.newFixedThreadPool(5);
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<String> records = List.of("one.one.one.one.",
                "dns.quad9.net.",
                "dns11.quad9.net.",
                "lookup1.resolver.lax-noc.com.",
                "b.resolvers.Level3.net.",
                "dns1.nextdns.io.",
                "dns2.nextdns.io.",
                "resolver.qwest.net.",
                "dns1.ncol.net.",
                "ny.ahadns.net.",
                "dns1.puregig.net.",
                "primary.dns.bright.net.",
                "edelta2.DADNS.america.net.",
                "ns2.frii.com.",
                "dns3.dejazzd.com.",
                "ns7.dns.tds.net.",
                "ns1.ceinetworks.com.",
                "nsorl.fdn.com.",
                "dns2.norlight.net.",
                "safeservedns.com.",
                "unkname.unk.edu.",
                "redirect.centurytel.net.",
                "dns2.nextdns.io.",
                "Edelta.DADNS.america.net.",
                "gatekeeper.poly.edu.",
                "ns1.wavecable.com.",
                "ns2.wavecable.com.",
                "nrcns.s3woodstock.ga.atlanta.comcast.net.",
                "resolver1.opendns.com.",
                "cns1.Atlanta2.Level3.net.",
                "redirect.centurytel.net.",
                "x.ns.gin.ntt.net.",
                "rec1pubns2.ultradns.net.",
                "dns2.dejazzd.com.",
                "c.resolvers.level3.net.",
                "dnscache2.izoom.net.",
                "ns2.nyc.pnap.net.",
                "yardbird.cns.vt.edu.",
                "cns4.Atlanta2.Level3.net.",
                "nscache.prserv.net.",
                "nscache07.us.prserv.net.",
                "hvdns1.centurylink.net.",
                "a.resolvers.level3.net.",
                "ns2.socket.net.",
                "res1.dns.cogentco.com.",
                "rdns.dynect.net.");

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

        System.out.println("Number of threads: " + numThreads);
        System.out.println("Number of records: " + records.size());
        System.out.println("Failed lookups: " + failureCount);

        assertThat(failureCount, equalTo(0));
    }

    private static void blockCommonPool() {
        try {
            Thread.sleep(1_000);
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