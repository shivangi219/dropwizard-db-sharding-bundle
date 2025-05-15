package io.appform.dropwizard.sharding.hibernate;

import io.dropwizard.Configuration;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
public class SessionFactoryManager<T extends Configuration> implements Managed {

    private final T configuration;
    private final Environment environment;
    private final List<SessionFactoryFactory<T>> factories;
    @Getter
    private final List<SessionFactorySource> sessionFactorySources = new ArrayList<>();

    public SessionFactoryManager(T configuration,
                                 Environment environment,
                                 List<SessionFactoryFactory<T>> factories) {
        this.configuration = configuration;
        this.environment = environment;
        this.factories = factories;
    }

    @Override
    public void start() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<CompletableFuture<SessionFactorySource>> futures = factories.stream()
                .map(factory -> CompletableFuture.supplyAsync(() -> {
                    try {
                        SessionFactorySource source = factory.build(configuration, environment);
                        source.getDataSource().start();
                        return source;
                    } catch (Exception e) {
                        throw new RuntimeException("Error initializing factory", e);
                    }
                }, executorService))
                .collect(Collectors.toList());
        List<SessionFactorySource> sources = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
        sessionFactorySources.addAll(sources);
    }


    @Override
    public void stop() throws Exception {
        for (SessionFactorySource sessionFactorySource : sessionFactorySources) {
            sessionFactorySource.getFactory().close();
            sessionFactorySource.getDataSource().stop();
        }
    }

}
