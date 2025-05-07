package io.appform.dropwizard.sharding.dao.operations.lookupdao;

import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.OpType;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.hibernate.Session;

import java.util.function.Function;

/**
 * Delete an entity by lookup Key.
 */
@Data
@Builder
public class DeleteByLookupKey extends OpContext<Boolean> {

    @NonNull
    private Function<String, Boolean> handler;
    @NonNull
    private String id;

    @Override
    public Boolean apply(Session session) {
        return handler.apply(id);
    }

    @Override
    public OpType getOpType() {
        return OpType.DELETE_BY_LOOKUP_KEY;
    }

    @Override
    public <R> R visit(OpContextVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
