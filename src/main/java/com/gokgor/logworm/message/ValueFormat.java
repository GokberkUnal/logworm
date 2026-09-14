package com.gokgor.logworm.message;

/** How the record value should be presented. */
public enum ValueFormat {
    /** Parse as JSON when the payload looks like JSON, otherwise fall back to a string. */
    AUTO,
    /** Always present the raw UTF-8 string. */
    STRING
}
