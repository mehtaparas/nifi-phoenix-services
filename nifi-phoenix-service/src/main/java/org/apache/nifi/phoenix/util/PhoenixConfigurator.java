package org.apache.nifi.phoenix.util;

/**
 * Created by parameht on 3/1/2018.
 */

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class PhoenixConfigurator {

    public Collection<ValidationResult> validate(String configFiles, String principal, String keyTab, AtomicReference<ValidationResources> validationResourceHolder, ComponentLog log) {

        final List<ValidationResult> problems = new ArrayList<>();
        ValidationResources resources = validationResourceHolder.get();

        // if no resources in the holder, or if the holder has different resources loaded,
        // then load the Configuration and set the new resources in the holder
        if (resources == null || !configFiles.equals(resources.getConfigResources())) {
            log.debug("Reloading validation resources");
            resources = new ValidationResources(configFiles, getConfigurationFromFiles(configFiles));
            validationResourceHolder.set(resources);
        }

        final Configuration hbaseConfig = resources.getConfiguration();

        problems.addAll(KerberosProperties.validatePrincipalAndKeytab(this.getClass().getSimpleName(), hbaseConfig, principal, keyTab, log));

        return problems;
    }

    public Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration hbaseConfig = HBaseConfiguration.create();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hbaseConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hbaseConfig;
    }

    public void preload(Configuration configuration) {
        try {
            FileSystem.get(configuration).close();
            UserGroupInformation.setConfiguration(configuration);
        } catch (IOException ioe) {
            // Suppress exception as future uses of this configuration will fail
        }
    }

    public UserGroupInformation authenticate(final Configuration hbaseConfig, String principal, String keyTab) throws AuthenticationFailedException {
        UserGroupInformation ugi;
        try {
            ugi = SecurityUtil.loginKerberos(hbaseConfig, principal, keyTab);
        } catch (IOException ioe) {
            throw new AuthenticationFailedException("Kerberos Authentication for Phoenix failed", ioe);
        }
        return ugi;
    }
}
