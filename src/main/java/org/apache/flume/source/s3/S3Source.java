/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source.s3;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * TODO
 * support Proxy options
 *
 */
public class S3Source extends AbstractSource implements
        Configurable, EventDrivenSource {

  private static final Logger logger = LoggerFactory
          .getLogger(S3Source.class);

  private String awsAccessKeyId;
  private String awsSecretKey;

  // Delay used when polling for new files
  private static final int POLL_DELAY_MS = 500;


  // TODO which option to use
  private String s3Buckets;
  private String endPoint;

  private SourceCounter sourceCounter;
  private ScheduledExecutorService executor;

  public static final int DEFAULT_BATCH_SIZE = 1000;

  private boolean backoff = true;
  private boolean hitChannelException = false;
  private int maxBackoff;
  S3ObjectEventReader reader;

  S3Service s3Client;

  private String completedSuffix;
  private String spoolDirectory;
  private boolean fileHeader;
  private String fileHeaderKey;
  private boolean basenameHeader;
  private String basenameHeaderKey;
  private int batchSize;
  private String ignorePattern;
  private String trackerDirPath;
  private String deserializerType;
  private Context deserializerContext;
  private String deletePolicy;
  private String inputCharset;
  private DecodeErrorPolicy decodeErrorPolicy;
  private volatile boolean hasFatalError = false;
  private SpoolDirectorySourceConfigurationConstants.ConsumeOrder consumeOrder;

  @Override
  public void configure(Context context) {
    awsAccessKeyId = context.getString(S3SourceConstants.AWS_ACCESS_KEY_ID);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(awsAccessKeyId),
            "AWS Key Id is required");

    awsSecretKey = context.getString(S3SourceConstants.AWS_SECRET_KEY);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(awsSecretKey),
            "AWS Secret Key must be specified");

    s3Buckets = context.getString(S3SourceConstants.S3_BUCKETS);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(s3Buckets),
            "Bucket name must be specified");

    endPoint = context.getString(S3SourceConstants.END_POINT);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(endPoint),
            "Endpoint cannot be null");

    batchSize = context.getInteger(S3SourceConstants.BATCH_SIZE, DEFAULT_BATCH_SIZE);

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public synchronized void start() {
    AWSCredentials awsCreds = new AWSCredentials(awsAccessKeyId, awsSecretKey);
    s3Client = new RestS3Service(awsCreds);

    try {
      reader = new S3ObjectEventReader.Builder()
              .spoolDirectory(s3Buckets)
              .completedSuffix(completedSuffix)
              .ignorePattern(ignorePattern)
              .trackerDirPath(trackerDirPath)
              .annotateFileName(fileHeader)
              .fileNameHeader(fileHeaderKey)
              .annotateBaseName(basenameHeader)
              .baseNameHeader(basenameHeaderKey)
              .deserializerType(deserializerType)
              .deserializerContext(deserializerContext)
              .deletePolicy(deletePolicy)
              .inputCharset(inputCharset)
              .decodeErrorPolicy(decodeErrorPolicy)
              .consumeOrder(consumeOrder)
              .build();
    } catch (IOException ioe) {
      throw new FlumeException("Error instantiating spooling event parser",
              ioe);
    }

    Runnable runner = new EventConsumerRunnable(reader, sourceCounter);
    executor.scheduleWithFixedDelay(
            runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("SpoolDirectorySource source started");
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {
  }

  private class EventConsumerRunnable implements Runnable {
    private S3ObjectEventReader reader;
    private SourceCounter sourceCounter;

    public EventConsumerRunnable(S3ObjectEventReader reader,
                                  SourceCounter sourceCounter) {
      this.reader = reader;
      this.sourceCounter = sourceCounter;
    }

    @Override
    public void run() {
      int backoffInterval = 250;
      try {
        while (!Thread.interrupted()) {
          List<Event> events = reader.readEvents(batchSize);
          if (events.isEmpty()) {
            break;
          }
          sourceCounter.addToEventReceivedCount(events.size());
          sourceCounter.incrementAppendBatchReceivedCount();

          try {
            getChannelProcessor().processEventBatch(events);
            reader.commit();
          } catch (ChannelException ex) {
            logger.warn("The channel is full, and cannot write data now. The " +
                    "source will try again after " + String.valueOf(backoffInterval) +
                    " milliseconds");
            hitChannelException = true;
            if (backoff) {
              TimeUnit.MILLISECONDS.sleep(backoffInterval);
              backoffInterval = backoffInterval << 1;
              backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
                      backoffInterval;
            }
            continue;
          }
          backoffInterval = 250;
          sourceCounter.addToEventAcceptedCount(events.size());
          sourceCounter.incrementAppendBatchAcceptedCount();
        }
      } catch (Throwable t) {
        logger.error("FATAL: " + this.toString() + ": " +
                "Uncaught exception in SpoolDirectorySource thread. " +
                "Restart or reconfigure Flume to continue processing.", t);
        hasFatalError = true;
        Throwables.propagate(t);
      }
    }
  }
}
