package com.mozafaq.extmergesort;

/**
 * @author Mozaffar Afaque
 */
public enum IOLocationType {
    AWS_S3_BUCKET,
    // Supported for output of sorted records
    RECORD_STREAM,
    FILE_SYSTEM;
}
