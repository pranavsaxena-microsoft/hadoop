package org.apache.hadoop.fs.azurebfs;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;

public class PerfTestAzureSetup extends PerfTestSetup {
    private static final Logger LOG =
            LoggerFactory.getLogger(PerfTestAzureSetup.class);

    private boolean isIPAddress;
    private NativeAzureFileSystem wasb;
    private AzureBlobFileSystem abfs;
    private String abfsScheme;

    private Configuration rawConfig;
    private AbfsConfiguration abfsConfig;
    private String fileSystemName;
    private String accountName;
    private String testUrl;
    private AuthType authType;


    public static final String FS_AZURE_ACCOUNT_NAME = "fs.azure.account.name";
    public static final String FS_AZURE_ABFS_ACCOUNT_NAME = "fs.azure.abfs.account.name";
    public static final String FS_AZURE_ACCOUNT_KEY = "fs.azure.account.key";
    public static final String FS_AZURE_CONTRACT_TEST_URI = "fs.contract.test.fs.abfs";
    public static final String FS_AZURE_TEST_APPENDBLOB_ENABLED = "fs.azure.test.appendblob.enabled";

    public static final String TEST_CONFIGURATION_FILE_NAME = "azure-test.xml";
    public static final String TEST_CONTAINER_PREFIX = "abfs-testcontainer-";
    public static final String MOCK_ACCOUNT_NAME = "mockAccount-c01112a3-2a23-433e-af2a-e808ea385136.blob.core.windows.net";
    public static final String WASB_ACCOUNT_NAME_DOMAIN_SUFFIX = ".blob.core.windows.net";
    public static final String MOCK_CONTAINER_NAME = "mockContainer";
    public static final String WASB_AUTHORITY_DELIMITER = "@";
    public static final String MOCK_WASB_URI = "wasb://" + MOCK_CONTAINER_NAME
            + WASB_AUTHORITY_DELIMITER + MOCK_ACCOUNT_NAME + "/";


    protected PerfTestAzureSetup() throws Exception {
        fileSystemName = TEST_CONTAINER_PREFIX + UUID.randomUUID().toString();
        System.out.println(fileSystemName);
        rawConfig = new Configuration();
        rawConfig.addResource(TEST_CONFIGURATION_FILE_NAME);

        this.accountName = rawConfig.get(FS_AZURE_ACCOUNT_NAME);
        if (accountName == null) {
            // check if accountName is set using different config key
            accountName = rawConfig.get(FS_AZURE_ABFS_ACCOUNT_NAME);
            System.out.println("accountname: " + accountName);
        }
        System.out.println("accountname: " + accountName);

        abfsConfig = new AbfsConfiguration(rawConfig, accountName);

        authType = abfsConfig.getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
        abfsScheme = authType == AuthType.SharedKey ? FileSystemUriSchemes.ABFS_SCHEME
                : FileSystemUriSchemes.ABFS_SECURE_SCHEME;

        if (authType == AuthType.SharedKey) {
            // Update credentials
        } else {

        }

        final String abfsUrl = this.getFileSystemName() + "@" + this.getAccountName();
        URI defaultUri = null;

        try {
            defaultUri = new URI(abfsScheme, abfsUrl, null, null, null);
        } catch (Exception ex) {
            throw new AssertionError(ex);
        }

        this.testUrl = defaultUri.toString();
        abfsConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());
        abfsConfig.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
        if (abfsConfig.get(FS_AZURE_TEST_APPENDBLOB_ENABLED) == "true") {
            String appendblobDirs = this.testUrl + "," + abfsConfig.get(FS_AZURE_CONTRACT_TEST_URI);
            rawConfig.set(FS_AZURE_APPEND_BLOB_KEY, appendblobDirs);
        }
        // For testing purposes, an IP address and port may be provided to override
        // the host specified in the FileSystem URI.  Also note that the format of
        // the Azure Storage Service URI changes from
        // http[s]://[account][domain-suffix]/[filesystem] to
        // http[s]://[ip]:[port]/[account]/[filesystem].
        String endPoint = abfsConfig.get(AZURE_ABFS_ENDPOINT);
        if (endPoint != null && endPoint.contains(":") && endPoint.split(":").length == 2) {
            this.isIPAddress = true;
        } else {
            this.isIPAddress = false;
        }
    }



    public void setup() throws Exception {
        createFileSystem();
    }

    public AzureBlobFileSystem getFileSystem() throws IOException {
        return abfs;
    }


    /**
     * Creates the filesystem; updates the {@link #abfs} field.
     * @return the created filesystem.
     * @throws IOException failure during create/init.
     */
    public AzureBlobFileSystem createFileSystem() throws IOException {
        if (abfs == null) {
            abfs = (AzureBlobFileSystem) FileSystem.newInstance(rawConfig);
        }
        return abfs;
    }



    protected String getFileSystemName() {
        return fileSystemName;
    }

    protected String getAccountName() {
        return this.accountName;
    }

    public AbfsConfiguration getConfiguration() {
        return abfsConfig;
    }


    @Override
    public void setBuffer(final int bufferSize) throws IOException {
        AbfsConfiguration abfsConfiguration = getFileSystem().getAbfsStore().getAbfsConfiguration();
        abfsConfiguration.setWriteBufferSize(bufferSize);
        abfsConfiguration.setReadBufferSize(bufferSize);
    }
}
