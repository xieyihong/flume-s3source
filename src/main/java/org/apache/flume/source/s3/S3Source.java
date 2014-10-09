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
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Source extends AbstractSource implements
        Configurable, EventDrivenSource {

  private static final Logger logger = LoggerFactory
          .getLogger(S3Source.class);

  private String awsAccessKeyId;
  private String awsSecretKey;
  private int batchSize;

  // TODO which option to use
  private String s3Buckets;
  private String endPoint;

  public static final int DEFAULT_BATCH_SIZE = 1000;

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
  }
}
