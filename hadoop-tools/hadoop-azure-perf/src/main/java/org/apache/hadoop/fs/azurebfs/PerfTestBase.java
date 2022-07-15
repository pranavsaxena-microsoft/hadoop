package org.apache.hadoop.fs.azurebfs;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.security.AbfsDelegationTokenManager;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.FILE_SYSTEM_NOT_FOUND;

public class PerfTestBase {
    private static final Logger LOG =
            LoggerFactory.getLogger(PerfTestBase.class);

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
    private boolean useConfiguredFileSystem = false;
    private boolean usingFilesystemForSASTests = false;
    private static final int SHORTENED_GUID_LEN = 12;


    public static final String FS_AZURE_ACCOUNT_NAME = "fs.azure.account.name";
    public static final String FS_AZURE_ABFS_ACCOUNT_NAME = "fs.azure.abfs.account.name";
    public static final String FS_AZURE_ACCOUNT_KEY = "fs.azure.account.key";
    public static final String FS_AZURE_CONTRACT_TEST_URI = "fs.contract.test.fs.abfs";
    public static final String FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT = "fs.azure.test.namespace.enabled";
    public static final String FS_AZURE_TEST_APPENDBLOB_ENABLED = "fs.azure.test.appendblob.enabled";
    public static final String FS_AZURE_TEST_CPK_ENABLED = "fs.azure.test.cpk.enabled";

    public static final String FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_ID = "fs.azure.account.oauth2.contributor.client.id";
    public static final String FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_SECRET = "fs.azure.account.oauth2.contributor.client.secret";

    public static final String FS_AZURE_BLOB_DATA_READER_CLIENT_ID = "fs.azure.account.oauth2.reader.client.id";
    public static final String FS_AZURE_BLOB_DATA_READER_CLIENT_SECRET = "fs.azure.account.oauth2.reader.client.secret";

    public static final String FS_AZURE_BLOB_FS_CLIENT_ID = "fs.azure.account.oauth2.client.id";
    public static final String FS_AZURE_BLOB_FS_CLIENT_SECRET = "fs.azure.account.oauth2.client.secret";

    public static final String FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID = "fs.azure.account.test.oauth2.client.id";
    public static final String FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET = "fs.azure.account.test.oauth2.client.secret";

    public static final String FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID = "fs.azure.check.access.testuser.guid";

    public static final String MOCK_SASTOKENPROVIDER_FAIL_INIT = "mock.sastokenprovider.fail.init";
    public static final String MOCK_SASTOKENPROVIDER_RETURN_EMPTY_SAS_TOKEN = "mock.sastokenprovider.return.empty.sasToken";

    public static final String FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID = "fs.azure.test.app.service.principal.tenant.id";

    public static final String FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_OBJECT_ID = "fs.azure.test.app.service.principal.object.id";

    public static final String FS_AZURE_TEST_APP_ID = "fs.azure.test.app.id";

    public static final String FS_AZURE_TEST_APP_SECRET = "fs.azure.test.app.secret";

    public static final String FS_AZURE_TEST_CPK_ENABLED_SECONDARY_ACCOUNT = "fs.azure.test.cpk-enabled-secondary-account";
    public static final String FS_AZURE_TEST_CPK_ENABLED_SECONDARY_ACCOUNT_KEY = "fs.azure.test.cpk-enabled-secondary-account.key";

    public static final String TEST_CONFIGURATION_FILE_NAME = "azure-test.xml";
    public static final String TEST_CONTAINER_PREFIX = "abfs-testcontainer-";
    public static final int TEST_TIMEOUT = 15 * 60 * 1000;

    private static final String SAS_PROPERTY_NAME = "fs.azure.sas.";

    public static final String ACCOUNT_KEY_PROPERTY_NAME = "fs.azure.account.key.";
    public static final String TEST_ACCOUNT_NAME_PROPERTY_NAME = "fs.azure.account.name";
    public static final String WASB_TEST_ACCOUNT_NAME_WITH_DOMAIN = "fs.azure.wasb.account.name";
    public static final String MOCK_ACCOUNT_NAME = "mockAccount-c01112a3-2a23-433e-af2a-e808ea385136.blob.core.windows.net";
    public static final String WASB_ACCOUNT_NAME_DOMAIN_SUFFIX = ".blob.core.windows.net";
    public static final String WASB_ACCOUNT_NAME_DOMAIN_SUFFIX_REGEX = "\\.blob(\\.preprod)?\\.core\\.windows\\.net";
    public static final String MOCK_CONTAINER_NAME = "mockContainer";
    public static final String WASB_AUTHORITY_DELIMITER = "@";
    public static final String WASB_SCHEME = "wasb";
    public static final String PATH_DELIMITER = "/";
    public static final String AZURE_ROOT_CONTAINER = "$root";
    public static final String MOCK_WASB_URI = "wasb://" + MOCK_CONTAINER_NAME
            + WASB_AUTHORITY_DELIMITER + MOCK_ACCOUNT_NAME + "/";
    private static final String USE_EMULATOR_PROPERTY_NAME = "fs.azure.test.emulator";

    private static final String KEY_DISABLE_THROTTLING = "fs.azure.disable.bandwidth.throttling";
    private static final String KEY_READ_TOLERATE_CONCURRENT_APPEND = "fs.azure.io.read.tolerate.concurrent.append";
    public static final String DEFAULT_PAGE_BLOB_DIRECTORY = "pageBlobs";
    public static final String DEFAULT_ATOMIC_RENAME_DIRECTORIES = "/atomicRenameDir1,/atomicRenameDir2";

    protected PerfTestBase() throws Exception {
        fileSystemName = TEST_CONTAINER_PREFIX + UUID.randomUUID().toString();
        rawConfig = new Configuration();
        rawConfig.addResource(TEST_CONFIGURATION_FILE_NAME);

        this.accountName = rawConfig.get(FS_AZURE_ACCOUNT_NAME);
        if (accountName == null) {
            // check if accountName is set using different config key
            accountName = rawConfig.get(FS_AZURE_ABFS_ACCOUNT_NAME);
        }

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

    protected boolean getIsNamespaceEnabled(AzureBlobFileSystem fs)
            throws IOException {
        return fs.getIsNamespaceEnabled(getTestTracingContext(fs, false));
    }

    public TracingContext getTestTracingContext(AzureBlobFileSystem fs,
                                                boolean needsPrimaryReqId) {
        String correlationId, fsId;
        TracingHeaderFormat format;
        if (fs == null) {
            correlationId = "test-corr-id";
            fsId = "test-filesystem-id";
            format = TracingHeaderFormat.ALL_ID_FORMAT;
        } else {
            AbfsConfiguration abfsConf = fs.getAbfsStore().getAbfsConfiguration();
            correlationId = abfsConf.getClientCorrelationId();
            fsId = fs.getFileSystemId();
            format = abfsConf.getTracingHeaderFormat();
        }
        return new TracingContext(correlationId, fsId,
                FSOperationType.TEST_OP, needsPrimaryReqId, format, null);
    }


    public void setup() throws Exception {
        //Create filesystem first to make sure getWasbFileSystem() can return an existing filesystem.
        createFileSystem();

        // Only live account without namespace support can run ABFS&WASB
        // compatibility tests
        if (!isIPAddress && (abfsConfig.getAuthType(accountName) != AuthType.SAS)
                && !abfs.getIsNamespaceEnabled(getTestTracingContext(
                getFileSystem(), false))) {
            final URI wasbUri = new URI(
                    abfsUrlToWasbUrl(getTestUrl(), abfsConfig.isHttpsAlwaysUsed()));
            final AzureNativeFileSystemStore azureNativeFileSystemStore =
                    new AzureNativeFileSystemStore();

            // update configuration with wasb credentials
            String accountNameWithoutDomain = accountName.split("\\.")[0];
            String wasbAccountName = accountNameWithoutDomain + WASB_ACCOUNT_NAME_DOMAIN_SUFFIX;
            String keyProperty = FS_AZURE_ACCOUNT_KEY + "." + wasbAccountName;
            if (rawConfig.get(keyProperty) == null) {
                rawConfig.set(keyProperty, getAccountKey());
            }

            azureNativeFileSystemStore.initialize(
                    wasbUri,
                    rawConfig,
                    new AzureFileSystemInstrumentation(rawConfig));

            wasb = new NativeAzureFileSystem(azureNativeFileSystemStore);
            wasb.initialize(wasbUri, rawConfig);
        }
    }

    public AzureBlobFileSystem getFileSystem() throws IOException {
        return abfs;
    }

    public AzureBlobFileSystem getFileSystem(Configuration configuration) throws Exception{
        final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(configuration);
        return fs;
    }

    public AzureBlobFileSystem getFileSystem(String abfsUri) throws Exception {
        abfsConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, abfsUri);
        final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(rawConfig);
        return fs;
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


    protected NativeAzureFileSystem getWasbFileSystem() {
        return wasb;
    }

    protected String getHostName() {
        // READ FROM ENDPOINT, THIS IS CALLED ONLY WHEN TESTING AGAINST DEV-FABRIC
        String endPoint = abfsConfig.get(AZURE_ABFS_ENDPOINT);
        return endPoint.split(":")[0];
    }

    protected void setTestUrl(String testUrl) {
        this.testUrl = testUrl;
    }

    protected String getTestUrl() {
        return testUrl;
    }


    protected String getFileSystemName() {
        return fileSystemName;
    }

    protected String getAccountName() {
        return this.accountName;
    }

    protected String getAccountKey() {
        return abfsConfig.get(FS_AZURE_ACCOUNT_KEY);
    }

    public AbfsConfiguration getConfiguration() {
        return abfsConfig;
    }

    public Configuration getRawConfiguration() {
        return abfsConfig.getRawConfiguration();
    }

    public AuthType getAuthType() {
        return this.authType;
    }

    public String getAbfsScheme() {
        return this.abfsScheme;
    }

    protected boolean isIPAddress() {
        return isIPAddress;
    }



    protected static String wasbUrlToAbfsUrl(final String wasbUrl) {
        return convertTestUrls(
                wasbUrl, FileSystemUriSchemes.WASB_SCHEME, FileSystemUriSchemes.WASB_SECURE_SCHEME, FileSystemUriSchemes.WASB_DNS_PREFIX,
                FileSystemUriSchemes.ABFS_SCHEME, FileSystemUriSchemes.ABFS_SECURE_SCHEME, FileSystemUriSchemes.ABFS_DNS_PREFIX, false);
    }

    protected static String abfsUrlToWasbUrl(final String abfsUrl, final boolean isAlwaysHttpsUsed) {
        return convertTestUrls(
                abfsUrl, FileSystemUriSchemes.ABFS_SCHEME, FileSystemUriSchemes.ABFS_SECURE_SCHEME, FileSystemUriSchemes.ABFS_DNS_PREFIX,
                FileSystemUriSchemes.WASB_SCHEME, FileSystemUriSchemes.WASB_SECURE_SCHEME, FileSystemUriSchemes.WASB_DNS_PREFIX, isAlwaysHttpsUsed);
    }

    private static String convertTestUrls(
            final String url,
            final String fromNonSecureScheme,
            final String fromSecureScheme,
            final String fromDnsPrefix,
            final String toNonSecureScheme,
            final String toSecureScheme,
            final String toDnsPrefix,
            final boolean isAlwaysHttpsUsed) {
        String data = null;
        if (url.startsWith(fromNonSecureScheme + "://") && isAlwaysHttpsUsed) {
            data = url.replace(fromNonSecureScheme + "://", toSecureScheme + "://");
        } else if (url.startsWith(fromNonSecureScheme + "://")) {
            data = url.replace(fromNonSecureScheme + "://", toNonSecureScheme + "://");
        } else if (url.startsWith(fromSecureScheme + "://")) {
            data = url.replace(fromSecureScheme + "://", toSecureScheme + "://");
        }

        if (data != null) {
            data = data.replace("." + fromDnsPrefix + ".",
                    "." + toDnsPrefix + ".");
        }
        return data;
    }

    public Path getTestPath() {
        Path path = new Path(UriUtils.generateUniqueTestPath());
        return path;
    }

    public AzureBlobFileSystemStore getAbfsStore(final AzureBlobFileSystem fs) {
        return fs.getAbfsStore();
    }

    public AbfsClient getAbfsClient(final AzureBlobFileSystemStore abfsStore) {
        return abfsStore.getClient();
    }

    public void setAbfsClient(AzureBlobFileSystemStore abfsStore,
                              AbfsClient client) {
        abfsStore.setClient(client);
    }

    public Path makeQualified(Path path) throws java.io.IOException {
        return getFileSystem().makeQualified(path);
    }

    /**
     * Create a path under the test path provided by
     * {@link #getTestPath()}.
     * @param filepath path string in
     * @return a path qualified by the test filesystem
     * @throws IOException IO problems
     */
    protected Path path(String filepath) throws IOException {
        return getFileSystem().makeQualified(
                new Path(getTestPath(), getUniquePath(filepath)));
    }

    /**
     * Generate a unique path using the given filepath.
     * @param filepath path string
     * @return unique path created from filepath and a GUID
     */
    protected Path getUniquePath(String filepath) {
        if (filepath.equals("/")) {
            return new Path(filepath);
        }
        return new Path(filepath + StringUtils
                .right(UUID.randomUUID().toString(), SHORTENED_GUID_LEN));
    }

    /**
     * Get any Delegation Token manager created by the filesystem.
     * @return the DT manager or null.
     * @throws IOException failure
     */
    protected AbfsDelegationTokenManager getDelegationTokenManager()
            throws IOException {
        return getFileSystem().getDelegationTokenManager();
    }

    /**
     * Generic create File and enabling AbfsOutputStream Flush.
     *
     * @param fs   AzureBlobFileSystem that is initialised in the test.
     * @param path Path of the file to be created.
     * @return AbfsOutputStream for writing.
     * @throws AzureBlobFileSystemException
     */
    protected AbfsOutputStream createAbfsOutputStreamWithFlushEnabled(
            AzureBlobFileSystem fs,
            Path path) throws IOException {
        AzureBlobFileSystemStore abfss = fs.getAbfsStore();
        abfss.getAbfsConfiguration().setDisableOutputStreamFlush(false);

        return (AbfsOutputStream) abfss.createFile(path, fs.getFsStatistics(),
                true, FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()),
                getTestTracingContext(fs, false));
    }

}
