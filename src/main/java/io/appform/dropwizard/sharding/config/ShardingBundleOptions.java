package io.appform.dropwizard.sharding.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShardingBundleOptions {

    private boolean skipReadOnlyTransaction = false;

    private boolean skipNativeHealthcheck = false;

    @Builder.Default
    private boolean encryptionSupportEnabled = false;

    private String encryptionAlgorithm;

    private String encryptionPassword;

    private String encryptionIv;

    /**
     * @deprecated If you are on MultiTenantBalancedDBShardingBundle or MultiTenantDBShardingBundle,
     * value from MultiTenantShardedHibernateFactory will be used.
     */
    @Deprecated
    @Builder.Default
    private long shardsInitializationTimeoutInSec = 60;

    /**
     * @deprecated If you are on MultiTenantBalancedDBShardingBundle or MultiTenantDBShardingBundle,
     * value from MultiTenantShardedHibernateFactory will be used.
     */
    @Deprecated
    @Builder.Default
    private int shardInitializationParallelism = 1;

}
