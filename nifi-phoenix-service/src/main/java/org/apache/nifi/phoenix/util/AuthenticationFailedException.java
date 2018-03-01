package org.apache.nifi.phoenix.util;

/**
 * Created by parameht on 3/1/2018.
 */

public class AuthenticationFailedException extends Exception {
    public AuthenticationFailedException(String reason, Exception cause) {
        super(reason, cause);
    }
}