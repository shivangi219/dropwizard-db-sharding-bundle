package io.appform.dropwizard.sharding.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class BlacklistingAwareHealthCheck extends HealthCheck {

    public BlacklistingAwareHealthCheck() {
    }

    @Override
    protected Result check() {
        return Result.healthy();
    }
}