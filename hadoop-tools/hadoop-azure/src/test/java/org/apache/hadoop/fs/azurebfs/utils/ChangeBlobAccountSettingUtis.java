package org.apache.hadoop.fs.azurebfs.utils;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET;

public class ChangeBlobAccountSettingUtis {

  private static ObjectMapper objectMapper = new ObjectMapper();

  public static void change(String subscrptionId,
      String accountId,
      Boolean versioning,
      Boolean softDelete,
      String resourceGroup,
      AbfsConfiguration configuration) throws Exception {
    URL url = new URL(
        "https://management.azure.com/subscriptions/" + subscrptionId
            + "/resourceGroups/" + resourceGroup
            + "/providers/Microsoft.Storage/storageAccounts/" + accountId
            + "/blobServices/default?api-version=2022-09-01");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("PUT");
    Body body = new Body();
    BodyProperty bodyProperty = new BodyProperty();
    BodyProperty.DeleteRetentionPolicy deleteRetentionPolicy
        = new BodyProperty.DeleteRetentionPolicy();
    deleteRetentionPolicy.enabled = softDelete;
    bodyProperty.isVersioningEnabled = versioning;
    bodyProperty.deleteRetentionPolicy = deleteRetentionPolicy;

    body.properties = bodyProperty;

    byte[] bytes = objectMapper.writeValueAsString(body).getBytes(
        StandardCharsets.UTF_8);

    String token = AzureADAuthenticator.getAzureTokenUsingClientCreds(
            configuration.get(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT),
            configuration.get(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID),
            configuration.get(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET))
        .getAccessToken();

    connection
        .setRequestProperty(HttpHeaderConfigurations.AUTHORIZATION,
            "Bearer " + token);

    try (OutputStream outputStream = connection.getOutputStream()) {
      outputStream.write(bytes);
    }

    if (connection.getResponseCode() != 200) {
      throw new Exception(
          "response is not 200" + connection.getResponseCode() + "; "
              + connection.getResponseMessage());
    }
    Thread.sleep(60_000L);
  }


  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Body {

    public BodyProperty getProperties() {
      return properties;
    }

    public void setProperties(final BodyProperty properties) {
      this.properties = properties;
    }

    BodyProperty properties;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class BodyProperty {

    public Boolean getIsVersioningEnabled() {
      return isVersioningEnabled;
    }

    public void setIsVersioningEnabled(final Boolean versioningEnabled) {
      isVersioningEnabled = versioningEnabled;
    }

    public DeleteRetentionPolicy getDeleteRetentionPolicy() {
      return deleteRetentionPolicy;
    }

    public void setDeleteRetentionPolicy(final DeleteRetentionPolicy deleteRetentionPolicy) {
      this.deleteRetentionPolicy = deleteRetentionPolicy;
    }

    Boolean isVersioningEnabled;

    DeleteRetentionPolicy deleteRetentionPolicy;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DeleteRetentionPolicy {

      public Boolean getEnabled() {
        return enabled;
      }

      public void setEnabled(final Boolean enabled) {
        this.enabled = enabled;
      }

      Boolean enabled;

      public int getDays() {
        return days;
      }

      public void setDays(final int days) {
        this.days = days;
      }

      int days = 10;
    }
  }
}
