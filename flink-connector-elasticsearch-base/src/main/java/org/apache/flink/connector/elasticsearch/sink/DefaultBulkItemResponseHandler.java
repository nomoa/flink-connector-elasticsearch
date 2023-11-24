package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.util.FlinkRuntimeException;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.rest.RestStatus;

import static org.apache.flink.connector.elasticsearch.sink.BulkItemResponseHandler.failure;

/**
 * TODO doc.
 *
 * @param <T>
 */
public class DefaultBulkItemResponseHandler<T> implements BulkItemResponseHandler<T> {
    @Override
    public Result<T> onResponse(DocWriteRequest<T> request, BulkItemResponse response) {
        if (response.isFailed()) {
            return failure(
                    wrapException(response.status(), response.getFailure().getCause(), request));
        }
        return BulkItemResponseHandler.success();
    }

    private static Throwable wrapException(
            RestStatus restStatus, Throwable rootFailure, DocWriteRequest<?> actionRequest) {
        if (restStatus == null) {
            return new FlinkRuntimeException(
                    String.format("Single action %s of bulk request failed.", actionRequest),
                    rootFailure);
        } else {
            return new FlinkRuntimeException(
                    String.format(
                            "Single action %s of bulk request failed with status %s.",
                            actionRequest, restStatus.getStatus()),
                    rootFailure);
        }
    }
}
