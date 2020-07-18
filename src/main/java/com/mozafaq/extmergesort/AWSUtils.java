package com.mozafaq.extmergesort;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

/**
 * @author Mozaffar Afaque
 */
public class AWSUtils {

    public static final String AWS_PROFILE_DEFAULT = "default";

    public static AWSCredentials createAWSCredentials(String awsProfile) {
        ProfileCredentialsProvider profileCredentialsProvider =
                new ProfileCredentialsProvider(awsProfile);
        return profileCredentialsProvider.getCredentials();
    }
}
