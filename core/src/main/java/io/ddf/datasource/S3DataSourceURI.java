package io.ddf.datasource;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by jing on 7/16/15.
 */
public class S3DataSourceURI extends DataSourceURI {
    private String awsKeyID = null;
    private String awsSecretKey = null;

    public S3DataSourceURI(String uri) throws URISyntaxException {
        super(new URI(uri));
    }

    public S3DataSourceURI(URI uri) {
        super(uri);
    }

    public void setAwsKeyID(String awsKeyID) {
        this.awsKeyID = awsKeyID;
    }

    public String getAwsKeyID() {
        return this.awsKeyID;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public String getAwsSecretKey() {
        return this.awsSecretKey;
    }

    @Override
    public URI getUri() {
        if (this.awsKeyID != null && this.awsSecretKey != null) {
            String creds = this.awsKeyID + ":" + this.awsSecretKey + "@";
            try {
                return new URI("s3n://" + creds + this.getUri().toString());
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        } else {
            try {
                return new URI("s3n://" + this.getUri().toString());
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}