package io.appform.dropwizard.sharding.hibernate;

import io.dropwizard.db.ManagedDataSource;
import lombok.Builder;
import lombok.Data;
import org.hibernate.SessionFactory;

@Builder
@Data
public class SessionFactorySource {
    private final SessionFactory factory;
    private final ManagedDataSource dataSource;
}
