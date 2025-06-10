package io.appform.dropwizard.sharding.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import io.appform.dropwizard.sharding.ShardInfoProvider;
import io.appform.dropwizard.sharding.config.ShardingBundleOptions;
import io.appform.dropwizard.sharding.sharding.NoopShardBlacklistingStore;
import io.appform.dropwizard.sharding.sharding.ShardBlacklistingStore;
import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class HealthCheckManager {

    private final String namespace;
    private final Environment environment;
    private final ShardInfoProvider shardInfoProvider;
    private final ShardBlacklistingStore blacklistingStore;
    private final ShardingBundleOptions shardingBundleOptions;

    private final Map<String, ShardHealthCheckMeta> dbHealthChecks = new ConcurrentHashMap<>();

    public HealthCheckManager(final String namespace,
                              final Environment environment,
                              final ShardInfoProvider shardInfoProvider,
                              final ShardBlacklistingStore blacklistingStore,
                              final ShardingBundleOptions shardingBundleOptions) {
        this.namespace = namespace;
        this.environment = environment;
        this.shardInfoProvider = shardInfoProvider;
        this.blacklistingStore = blacklistingStore;
        this.shardingBundleOptions = shardingBundleOptions;
    }

    public void register(final String name,
                         final HealthCheck healthCheck) {
        if (shardingBundleOptions.isSkipNativeHealthcheck()) {
            return;
        }
        String dbNamespace = shardInfoProvider.namespace(name);
        if (!Objects.equals(dbNamespace, this.namespace)) {
            return;
        }
        int shardId = shardInfoProvider.shardId(name);
        if (shardId == -1) {
            return;
        }
        HealthCheck hc = null;
        if (!(blacklistingStore instanceof NoopShardBlacklistingStore)) {
            hc = new BlacklistingAwareHealthCheck();
        } else {
            hc = healthCheck;
        }
        dbHealthChecks.put(name, ShardHealthCheckMeta.builder()
                .healthCheck(hc)
                .shardId(shardId)
                .build());
        environment.healthChecks().register(name, hc);

    }

    public Map<Integer, Boolean> status() {
        return dbHealthChecks.values()
                .stream()
                .map(shardHealthCheckMeta -> new AbstractMap.SimpleEntry<>(shardHealthCheckMeta.getShardId(),
                        shardHealthCheckMeta.getHealthCheck().execute().isHealthy()))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }
}
