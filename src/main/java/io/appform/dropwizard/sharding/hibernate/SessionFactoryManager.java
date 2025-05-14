package io.appform.dropwizard.sharding.hibernate;

import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class SessionFactoryManager implements Managed {

    private final List<SessionFactorySource> sessionFactorySources;

    public SessionFactoryManager(
            List<SessionFactorySource> sessionFactorySources) {
        this.sessionFactorySources = sessionFactorySources;
    }

    @Override
    public void start() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();
        CompletableFuture.allOf(sessionFactorySources.stream()
                .map(sessionFactorySource -> CompletableFuture.runAsync(() -> {
                    try {
                        sessionFactorySource.getDataSource().start();
                    } catch (Exception e) {
                        log.error("Error initialising db sharding bundle", e);
                        throw new RuntimeException(e);
                    }
                }, executorService)).toArray(CompletableFuture[]::new)).join();
    }


    @Override
    public void stop() throws Exception {
        for (SessionFactorySource sessionFactorySource : sessionFactorySources) {
            sessionFactorySource.getFactory().close();
            sessionFactorySource.getDataSource().stop();
        }

    }
}
