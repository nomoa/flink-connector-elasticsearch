package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;

import java.io.Serializable;

/**
 * TODO doc.
 *
 * @param <T>
 */
@PublicEvolving
public interface BulkItemResponseHandler<T> extends Serializable {
    /**
     * TODO doc.
     *
     * @param context
     */
    default void open(Sink.InitContext context) {
        // purpose is mainly to allow the handler to access the metricGroup and declare new metrics
    }

    /**
     * TODO doc.
     *
     * @param request
     * @param response
     * @return
     */
    Result<T> onResponse(DocWriteRequest<T> request, BulkItemResponse response);

    /** TODO doc. */
    @PublicEvolving
    enum Status {
        SUCCESS,
        RETRY,
        ERROR
    }

    /**
     * TODO doc.
     *
     * @param <T>
     */
    @PublicEvolving
    interface Result<T> {
        /**
         * TODO doc.
         *
         * @return
         */
        Status status();

        default DocWriteRequest<T> retryAction() {
            throw new UnsupportedOperationException();
        }

        default Throwable failure() {
            throw new UnsupportedOperationException();
        }
    }

    static <E> Result<E> retry(DocWriteRequest<E> request) {
        return new Result<E>() {
            @Override
            public Status status() {
                return Status.RETRY;
            }

            @Override
            public DocWriteRequest<E> retryAction() {
                return request;
            }
        };
    }

    static <E> Result<E> failure(Throwable failure) {
        return new Result<E>() {
            @Override
            public Status status() {
                return Status.ERROR;
            }

            public Throwable failure() {
                return failure;
            }
        };
    }

    static <E> Result<E> success() {
        return () -> Status.SUCCESS;
    }
}
