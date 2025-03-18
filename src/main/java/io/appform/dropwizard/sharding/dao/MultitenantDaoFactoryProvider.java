package io.appform.dropwizard.sharding.dao;

import io.appform.dropwizard.sharding.ShardInfoProvider;
import io.appform.dropwizard.sharding.config.ShardingBundleOptions;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import io.appform.dropwizard.sharding.sharding.ShardManager;
import org.hibernate.SessionFactory;

import java.util.List;
import java.util.Map;

/**
 * Factory provider to create instances of MultiTenantDaoFactory.
 */
public class MultitenantDaoFactoryProvider {

    public static MultiTenantDaoFactory create(
            Map<String, List<SessionFactory>> sessionFactories,
            Map<String, ShardManager> shardManagers,
            Map<String, ShardingBundleOptions> shardingOptions,
            Map<String, ShardInfoProvider> shardInfoProviders,
            TransactionObserver observer) {
        return new DefaultMultiTenantDaoFactory(
                sessionFactories,
                shardManagers,
                shardingOptions,
                shardInfoProviders,
                observer
        );
    }

}
