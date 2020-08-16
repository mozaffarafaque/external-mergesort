package com.mozafaq.extmergesort;

/**
 * @author Mozaffar Afaque
 */
public enum IOType {
    AWS_S3_BUCKET,
    // Only supported for output of sorted records
    RECORD_STREAM,
    FILE_SYSTEM;
}
