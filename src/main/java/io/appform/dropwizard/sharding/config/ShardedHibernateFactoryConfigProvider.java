package io.appform.dropwizard.sharding.config;

import java.util.Map;

public interface ShardedHibernateFactoryConfigProvider {

    ShardedHibernateFactory getForTenant(final String tenantId);

    Map<String, ShardedHibernateFactory> listAll();

}
