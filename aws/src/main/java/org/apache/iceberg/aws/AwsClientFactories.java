/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aws;

import java.net.URI;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

public class AwsClientFactories {

  private static final DefaultAwsClientFactory AWS_CLIENT_FACTORY_DEFAULT = new DefaultAwsClientFactory();

  private AwsClientFactories() {
  }

  public static AwsClientFactory defaultFactory() {
    return AWS_CLIENT_FACTORY_DEFAULT;
  }

  public static AwsClientFactory from(Map<String, String> properties) {
    String factoryImpl = PropertyUtil.propertyAsString(
        properties, AwsProperties.CLIENT_FACTORY, DefaultAwsClientFactory.class.getName());
    return loadClientFactory(factoryImpl, properties);
  }

  private static AwsClientFactory loadClientFactory(String impl, Map<String, String> properties) {
    DynConstructors.Ctor<AwsClientFactory> ctor;
    try {
      ctor = DynConstructors.builder(AwsClientFactory.class).hiddenImpl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(String.format(
          "Cannot initialize AwsClientFactory, missing no-arg constructor: %s", impl), e);
    }

    AwsClientFactory factory;
    try {
      factory = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize AwsClientFactory, %s does not implement AwsClientFactory.", impl), e);
    }

    factory.initialize(properties);
    return factory;
  }

  static class DefaultAwsClientFactory implements AwsClientFactory {

    private String s3Endpoint;
    private String s3AccessKeyId;
    private String s3SecretAccessKey;
    private String s3SessionToken;

    DefaultAwsClientFactory() {
    }

    @Override
    public S3Client s3() {
      return S3Client.builder()
          .httpClientBuilder(UrlConnectionHttpClient.builder())
          .applyMutation(builder -> configureEndpoint(builder, s3Endpoint))
          .credentialsProvider(credentialsProvider(s3AccessKeyId, s3SecretAccessKey, s3SessionToken))
          .build();
    }

    @Override
    public GlueClient glue() {
      return GlueClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build();
    }

    @Override
    public KmsClient kms() {
      return KmsClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build();
    }

    @Override
    public DynamoDbClient dynamo() {
      return DynamoDbClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build();
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.s3Endpoint = properties.get(AwsProperties.S3FILEIO_ENDPOINT);
      this.s3AccessKeyId = properties.get(AwsProperties.S3FILEIO_ACCESS_KEY_ID);
      this.s3SecretAccessKey = properties.get(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY);
      this.s3SessionToken = properties.get(AwsProperties.S3FILEIO_SESSION_TOKEN);

      ValidationException.check((s3AccessKeyId == null && s3SecretAccessKey == null) ||
          (s3AccessKeyId != null && s3SecretAccessKey != null),
          "S3 client access key ID and secret access key must be set at the same time");
    }
  }

  static <T extends SdkClientBuilder> void configureEndpoint(T builder, String endpoint) {
    if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
    }
  }

  static AwsCredentialsProvider credentialsProvider(
      String accessKeyId, String secretAccessKey, String sessionToken) {
    if (accessKeyId != null) {
      if (sessionToken == null) {
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKeyId, secretAccessKey));
      } else {
        return StaticCredentialsProvider.create(
            AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
      }
    } else {
      return DefaultCredentialsProvider.create();
    }
  }
}
