package io.appform.dropwizard.sharding.utils;

import io.appform.dropwizard.sharding.query.QuerySpec;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.hibernate.Session;
import org.hibernate.query.Query;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.io.*;

/**
 * Utilities for internal use
 */
@UtilityClass
public class InternalUtils {
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T cloneObject(final T input) {
        try (val bos = new ByteArrayOutputStream(); val os = new ObjectOutputStream(bos)) {
            os.writeObject(input);
            os.flush();

            try (val ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
                return (T) ois.readObject();
            }
        }
    }

    public <T> Query<T> createQuery(
            final Session session,
            final Class<T> entityClass,
            final QuerySpec<T, T> querySpec) {
        CriteriaBuilder builder = session.getCriteriaBuilder();
        CriteriaQuery<T> criteria = builder.createQuery(entityClass);
        Root<T> root = criteria.from(entityClass);
        querySpec.apply(root, criteria, builder);
        return session.createQuery(criteria);
    }
}
