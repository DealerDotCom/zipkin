/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin.Codec;
import zipkin.Span;
import zipkin.internal.ApplyTimestampAndDuration;
import zipkin.storage.guava.GuavaSpanConsumer;

import static com.google.common.util.concurrent.Futures.transform;
import static zipkin.storage.cassandra.CassandraUtil.annotationKeys;
import static zipkin.storage.cassandra.CassandraUtil.bindWithName;
import static zipkin.storage.cassandra.CassandraUtil.durationIndexBucket;
import static zipkin.storage.cassandra.CassandraUtil.iso8601;

final class CassandraSpanConsumer implements GuavaSpanConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSpanConsumer.class);
  private static final long WRITTEN_NAMES_TTL
      = Long.getLong("zipkin.store.cassandra.internal.writtenNamesTtl", 60 * 60 * 1000);

  private static final Function<Object, Void> TO_VOID = Functions.<Void>constant(null);
  private static final Random RAND = new Random();

  private final Session session;
  private final TimestampCodec timestampCodec;
  private final int bucketCount;
  @Deprecated
  private final int spanTtl;
  @Deprecated
  private final int indexTtl;
  private final PreparedStatement insertSpan;
  private final PreparedStatement insertServiceName;
  private final PreparedStatement insertSpanName;
  private final PreparedStatement insertTraceIdByServiceName;
  private final PreparedStatement insertTraceIdBySpanName;
  private final PreparedStatement insertTraceIdByAnnotation;
  private final PreparedStatement insertTraceIdBySpanDuration;
  private final Schema.Metadata metadata;

  private final ThreadLocal<Set<String>> writtenNames = new ThreadLocal<Set<String>>() {
    private long cacheInterval = toCacheInterval(System.currentTimeMillis());

    @Override
    protected Set<String> initialValue() {
      return new HashSet<>();
    }

    @Override
    public Set<String> get() {
      long newCacheInterval = toCacheInterval(System.currentTimeMillis());
      if (cacheInterval != newCacheInterval) {
        cacheInterval = newCacheInterval;
        set(new HashSet<String>());
      }
      return super.get();
    }

    private long toCacheInterval(long ms) {
      return ms / WRITTEN_NAMES_TTL;
    }
  };

  CassandraSpanConsumer(Session session, int bucketCount, int spanTtl, int indexTtl) {
    this.session = session;
    this.timestampCodec = new TimestampCodec(session);
    this.bucketCount = bucketCount;
    this.spanTtl = spanTtl;
    this.indexTtl = indexTtl;
    this.metadata = Schema.readMetadata(session);
    insertSpan = session.prepare(
        maybeUseTtl(QueryBuilder
            .insertInto("traces")
            .value("trace_id", QueryBuilder.bindMarker("trace_id"))
            .value("ts", QueryBuilder.bindMarker("ts"))
            .value("span_name", QueryBuilder.bindMarker("span_name"))
            .value("span", QueryBuilder.bindMarker("span"))));

    insertServiceName = session.prepare(
        maybeUseTtl(QueryBuilder
            .insertInto("service_names")
            .value("service_name", QueryBuilder.bindMarker("service_name"))));

    insertSpanName = session.prepare(
        maybeUseTtl(QueryBuilder
            .insertInto("span_names")
            .value("service_name", QueryBuilder.bindMarker("service_name"))
            .value("bucket", QueryBuilder.bindMarker("bucket"))
            .value("span_name", QueryBuilder.bindMarker("span_name"))));

    insertTraceIdByServiceName = session.prepare(
        maybeUseTtl(QueryBuilder
            .insertInto("service_name_index")
            .value("service_name", QueryBuilder.bindMarker("service_name"))
            .value("bucket", QueryBuilder.bindMarker("bucket"))
            .value("ts", QueryBuilder.bindMarker("ts"))
            .value("trace_id", QueryBuilder.bindMarker("trace_id"))));

    insertTraceIdBySpanName = session.prepare(
        maybeUseTtl(QueryBuilder
            .insertInto("service_span_name_index")
            .value("service_span_name", QueryBuilder.bindMarker("service_span_name"))
            .value("ts", QueryBuilder.bindMarker("ts"))
            .value("trace_id", QueryBuilder.bindMarker("trace_id"))));

    insertTraceIdByAnnotation = session.prepare(
        maybeUseTtl(QueryBuilder
            .insertInto("annotations_index")
            .value("annotation", QueryBuilder.bindMarker("annotation"))
            .value("bucket", QueryBuilder.bindMarker("bucket"))
            .value("ts", QueryBuilder.bindMarker("ts"))
            .value("trace_id", QueryBuilder.bindMarker("trace_id"))));

    insertTraceIdBySpanDuration = session.prepare(
        maybeUseTtl(QueryBuilder
            .insertInto("span_duration_index")
            .value("service_name", QueryBuilder.bindMarker("service_name"))
            .value("span_name", QueryBuilder.bindMarker("span_name"))
            .value("bucket", QueryBuilder.bindMarker("bucket"))
            .value("duration", QueryBuilder.bindMarker("duration"))
            .value("ts", QueryBuilder.bindMarker("ts"))
            .value("trace_id", QueryBuilder.bindMarker("trace_id"))));
  }

  private RegularStatement maybeUseTtl(Insert value) {
    return metadata.hasDefaultTtl
        ? value
        : value.using(QueryBuilder.ttl(QueryBuilder.bindMarker("ttl_")));
  }

  /**
   * This fans out into many requests, last count was 8 * spans.size. If any of these fail, the
   * returned future will fail. Most callers drop or log the result.
   */
  @Override
  public ListenableFuture<Void> accept(List<Span> spans) {
    List<ListenableFuture<?>> futures = new LinkedList<>();
    for (Span span : spans) {
      span = ApplyTimestampAndDuration.apply(span);
      futures.add(storeSpan(
          span.traceId,
          span.timestamp != null ? span.timestamp : 0L,
          String.format("%d_%d_%d",
              span.id,
              span.annotations.hashCode(),
              span.binaryAnnotations.hashCode()),
          ByteBuffer.wrap(Codec.THRIFT.writeSpan(span))));

      for (String serviceName : span.serviceNames()) {
        // SpanStore.getServiceNames
        futures.add(storeServiceName(serviceName));
        if (!span.name.isEmpty()) {
          // SpanStore.getSpanNames
          futures.add(storeSpanName(serviceName, span.name));
        }

        if (span.timestamp != null) {
          // QueryRequest.serviceName
          futures.add(storeTraceIdByServiceName(serviceName, span.timestamp, span.traceId));

          // QueryRequest.spanName
          if (!span.name.isEmpty()) {
            futures.add(storeTraceIdBySpanName(
                serviceName, span.name, span.timestamp, span.traceId));
          }

          // QueryRequest.min/maxDuration
          if (span.duration != null) {
            // Contract for Repository.storeTraceIdByDuration is to store the span twice, once with
            // the span name and another with empty string.
            futures.add(storeTraceIdByDuration(
                serviceName, span.name, span.timestamp, span.duration, span.traceId));
            if (!span.name.isEmpty()) { // If span.name == "", this would be redundant
              futures.add(storeTraceIdByDuration(
                  serviceName, "", span.timestamp, span.duration, span.traceId));
            }
          }
        }
      }
      // QueryRequest.annotations/binaryAnnotations
      if (span.timestamp != null) {
        for (String annotation : annotationKeys(span)) {
          futures.add(storeTraceIdByAnnotation(annotation, span.timestamp, span.traceId));
        }
      }
    }
    return transform(Futures.allAsList(futures), TO_VOID);
  }

  /**
   * Store the span in the underlying storage for later retrieval.
   */
  ListenableFuture<?> storeSpan(long traceId, long timestamp, String spanName, ByteBuffer span) {
    Preconditions.checkNotNull(spanName);
    Preconditions.checkArgument(!spanName.isEmpty());

    try {
      if (0 == timestamp && metadata.compactionClass.contains("DateTieredCompactionStrategy")) {
        LOG.warn("Span {} in trace {} had no timestamp. "
            + "If this happens a lot consider switching back to SizeTieredCompactionStrategy for "
            + "{}.traces", spanName, traceId, session.getLoggedKeyspace());
      }

      BoundStatement bound = bindWithName(insertSpan, "insert-span")
          .setLong("trace_id", traceId)
          .setBytesUnsafe("ts", timestampCodec.serialize(timestamp))
          .setString("span_name", spanName)
          .setBytes("span", span);
      if (!metadata.hasDefaultTtl) bound.setInt("ttl_", spanTtl);

      if (LOG.isDebugEnabled()) LOG.debug(debugInsertSpan(traceId, timestamp, spanName, span));

      return session.executeAsync(bound);
    } catch (RuntimeException ex) {
      LOG.error("failed " + debugInsertSpan(traceId, timestamp, spanName, span), ex);
      return Futures.immediateFailedFuture(ex);
    }
  }

  private String debugInsertSpan(long traceId, long timestamp, String spanName, ByteBuffer span) {
    return insertSpan.getQueryString()
        .replace(":trace_id", String.valueOf(traceId))
        .replace(":ts", String.valueOf(timestamp))
        .replace(":span_name", spanName)
        .replace(":span", Bytes.toHexString(span))
        .replace(":ttl_", String.valueOf(spanTtl)); // no-op when using default ttls
  }

  ListenableFuture<?> storeServiceName(String serviceName) {
    Preconditions.checkNotNull(serviceName);
    Preconditions.checkArgument(!serviceName.isEmpty());
    if (writtenNames.get().add(serviceName)) {
      try {
        BoundStatement bound = bindWithName(insertServiceName, "insert-service-name")
            .setString("service_name", serviceName);
        if (!metadata.hasDefaultTtl) bound.setInt("ttl_", indexTtl);

        if (LOG.isDebugEnabled()) LOG.debug(debugInsertServiceName(serviceName));

        return session.executeAsync(bound);
      } catch (RuntimeException ex) {
        LOG.error("failed " + debugInsertServiceName(serviceName), ex);
        writtenNames.get().remove(serviceName);
        throw ex;
      }
    } else {
      return Futures.immediateFuture(null);
    }
  }

  private String debugInsertServiceName(String serviceName) {
    return insertServiceName.getQueryString()
        .replace(":service_name", serviceName)
        .replace(":ttl_", String.valueOf(indexTtl)); // no-op when using default ttls
  }

  ListenableFuture<?> storeSpanName(String serviceName, String spanName) {
    Preconditions.checkNotNull(serviceName);
    Preconditions.checkArgument(!serviceName.isEmpty());
    Preconditions.checkNotNull(spanName);
    Preconditions.checkArgument(!spanName.isEmpty());
    int bucket = 0;
    if (writtenNames.get().add(serviceName + "––" + spanName)) {
      try {
        BoundStatement bound = bindWithName(insertSpanName, "insert-span-name")
            .setString("service_name", serviceName)
            .setInt("bucket", bucket)
            .setString("span_name", spanName);
        if (!metadata.hasDefaultTtl) bound.setInt("ttl_", indexTtl);

        if (LOG.isDebugEnabled()) LOG.debug(debugInsertSpanName(bucket, serviceName, spanName));

        return session.executeAsync(bound);
      } catch (RuntimeException ex) {
        LOG.error("failed " + debugInsertSpanName(bucket, serviceName, spanName), ex);
        writtenNames.get().remove(serviceName + "––" + spanName);
        return Futures.immediateFailedFuture(ex);
      }
    } else {
      return Futures.immediateFuture(null);
    }
  }

  private String debugInsertSpanName(int bucket, String serviceName, String spanName) {
    return insertSpanName.getQueryString()
        .replace(":bucket", String.valueOf(bucket))
        .replace(":service_name", serviceName)
        .replace(":span_name", spanName)
        .replace(":ttl_", String.valueOf(indexTtl)); // no-op when using default ttls
  }

  ListenableFuture<?> storeTraceIdByServiceName(String serviceName, long timestamp, long traceId) {
    Preconditions.checkNotNull(serviceName);
    Preconditions.checkArgument(!serviceName.isEmpty());
    int bucket = RAND.nextInt(bucketCount);
    try {
      BoundStatement bound =
          bindWithName(insertTraceIdByServiceName, "insert-trace-id-by-service-name")
              .setInt("bucket", bucket)
              .setString("service_name", serviceName)
              .setBytesUnsafe("ts", timestampCodec.serialize(timestamp))
              .setLong("trace_id", traceId);
      if (!metadata.hasDefaultTtl) bound.setInt("ttl_", indexTtl);

      if (LOG.isDebugEnabled()) {
        LOG.debug(debugInsertTraceIdByServiceName(bucket, serviceName, timestamp, traceId));
      }

      return session.executeAsync(bound);
    } catch (RuntimeException ex) {
      LOG.error(
          "failed " + debugInsertTraceIdByServiceName(bucket, serviceName, timestamp, traceId), ex);
      return Futures.immediateFailedFuture(ex);
    }
  }

  private String debugInsertTraceIdByServiceName(int bucket, String serviceName, long timestamp,
      long traceId) {
    return insertTraceIdByServiceName.getQueryString()
        .replace(":bucket", String.valueOf(bucket))
        .replace(":service_name", serviceName)
        .replace(":ts", iso8601(timestamp))
        .replace(":trace_id", String.valueOf(traceId))
        .replace(":ttl_", String.valueOf(indexTtl)); // no-op when using default ttls
  }

  ListenableFuture<?> storeTraceIdBySpanName(String serviceName, String spanName, long timestamp,
      long traceId) {
    Preconditions.checkNotNull(serviceName);
    Preconditions.checkArgument(!serviceName.isEmpty());
    Preconditions.checkNotNull(spanName);
    Preconditions.checkArgument(!spanName.isEmpty());
    String serviceSpanName = serviceName + "." + spanName;

    try {
      BoundStatement bound = bindWithName(insertTraceIdBySpanName, "insert-trace-id-by-span-name")
          .setString("service_span_name", serviceSpanName)
          .setBytesUnsafe("ts", timestampCodec.serialize(timestamp))
          .setLong("trace_id", traceId);
      if (!metadata.hasDefaultTtl) bound.setInt("ttl_", indexTtl);

      if (LOG.isDebugEnabled()) {
        LOG.debug(debugInsertTraceIdBySpanName(serviceSpanName, timestamp, traceId));
      }
      return session.executeAsync(bound);
    } catch (RuntimeException ex) {
      LOG.error("failed " + debugInsertTraceIdBySpanName(serviceSpanName, timestamp, traceId), ex);
      return Futures.immediateFailedFuture(ex);
    }
  }

  private String debugInsertTraceIdBySpanName(String serviceSpanName, long timestamp, long traceId) {
    return insertTraceIdBySpanName.getQueryString()
        .replace(":service_span_name", serviceSpanName)
        .replace(":ts", String.valueOf(timestamp))
        .replace(":trace_id", String.valueOf(traceId))
        .replace(":ttl_", String.valueOf(indexTtl)); // no-op when using default ttls
  }

  ListenableFuture<?> storeTraceIdByAnnotation(String annotationKey, long timestamp, long traceId) {
    int bucket = RAND.nextInt(bucketCount);
    try {
      BoundStatement bound = bindWithName(insertTraceIdByAnnotation, "insert-trace-id-by-annotation")
          .setInt("bucket", bucket)
          .setBytes("annotation", CassandraUtil.toByteBuffer(annotationKey))
          .setBytesUnsafe("ts", timestampCodec.serialize(timestamp))
          .setLong("trace_id", traceId);
      if (!metadata.hasDefaultTtl) bound.setInt("ttl_", indexTtl);

      if (LOG.isDebugEnabled()) {
        LOG.debug(debugInsertTraceIdByAnnotation(bucket, annotationKey, timestamp, traceId));
      }
      return session.executeAsync(bound);
    } catch (CharacterCodingException | RuntimeException ex) {
      LOG.error(
          "failed " + debugInsertTraceIdByAnnotation(bucket, annotationKey, timestamp, traceId),
          ex);
      return Futures.immediateFailedFuture(ex);
    }
  }

  private String debugInsertTraceIdByAnnotation(int bucket, String annotationKey, long timestamp,
      long traceId) {
    return insertTraceIdByAnnotation.getQueryString()
        .replace(":bucket", String.valueOf(bucket))
        .replace(":annotation", annotationKey)
        .replace(":ts", iso8601(timestamp))
        .replace(":trace_id", String.valueOf(traceId))
        .replace(":ttl_", String.valueOf(indexTtl)); // no-op when using default ttls
  }

  ListenableFuture<?> storeTraceIdByDuration(String serviceName, String spanName,
      long timestamp, long duration, long traceId) {
    int bucket = durationIndexBucket(timestamp);
    try {
      BoundStatement bound =
          bindWithName(insertTraceIdBySpanDuration, "insert-trace-id-by-span-duration")
              .setInt("bucket", bucket)
              .setString("service_name", serviceName)
              .setString("span_name", spanName)
              .setBytesUnsafe("ts", timestampCodec.serialize(timestamp))
              .setLong("duration", duration)
              .setLong("trace_id", traceId);
      if (!metadata.hasDefaultTtl) bound.setInt("ttl_", indexTtl);

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            debugInsertTraceIdBySpanDuration(bucket, serviceName, spanName, timestamp, duration,
                traceId));
      }
      return session.executeAsync(bound);
    } catch (RuntimeException ex) {
      LOG.error(
          "failed " + debugInsertTraceIdBySpanDuration(bucket, serviceName, spanName, timestamp,
              duration, traceId));
      return Futures.immediateFailedFuture(ex);
    }
  }

  private String debugInsertTraceIdBySpanDuration(int bucket, String serviceName, String spanName,
      long timestamp, long duration, long traceId) {
    return insertTraceIdBySpanDuration.getQueryString()
        .replace(":bucket", String.valueOf(bucket))
        .replace(":service_name", serviceName)
        .replace(":span_name", spanName)
        .replace(":ts", iso8601(timestamp))
        .replace(":duration", String.valueOf(duration))
        .replace(":trace_id", String.valueOf(traceId))
        .replace(":ttl_", String.valueOf(indexTtl)); // no-op when using default ttls
  }
}
