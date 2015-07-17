package io.ddf.datasource;

/**
 * Created by jing on 7/16/15.
 */
public class S3DataSourceCredentials implements IDataSourceCredentials {
    private String credentials;
    private String awsKeyID;
    private String awsScretKey;

    public S3DataSourceCredentials(String awsKeyID, String awsSecretKey) {
        this.awsKeyID = awsKeyID;
        this.awsScretKey = awsSecretKey;
        this.credentials = awsKeyID + ":" + awsSecretKey;
    }

    /**
     * @brief Getters and Setters.
     */

    public String getCredentials() {
        return credentials;
    }

    public void setCredentials(String credentials) {
        this.credentials = credentials;
    }

    public String getAwsKeyID() {
        return awsKeyID;
    }

    public void setAwsKeyID(String awsKeyID) {
        this.awsKeyID = awsKeyID;
    }

    public String getAwsScretKey() {
        return awsScretKey;
    }

    public void setAwsScretKey(String awsScretKey) {
        this.awsScretKey = awsScretKey;
    }
}