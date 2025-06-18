package io.appform.dropwizard.sharding.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TenantShardingBundleOptions {
    private boolean skipReadOnlyTransaction = false;

    private boolean skipNativeHealthcheck = false;

    @Builder.Default
    private boolean encryptionSupportEnabled = false;

    private String encryptionAlgorithm;

    private String encryptionPassword;

    private String encryptionIv;

}
