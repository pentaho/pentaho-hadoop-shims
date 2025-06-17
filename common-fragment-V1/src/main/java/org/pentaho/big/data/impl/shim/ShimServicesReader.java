/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.impl.shim;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * ShimServicesReader is a utility class that reads a YAML file named "shim_services.yaml"
 * from the classpath and provides methods to retrieve service names, HDFS schemas,
 * service options, and Hive drivers.
 *
 * The YAML file should contain a structure with a "services" key, where each service
 * can have various attributes including "hdfs" and "hive".
 */
public class ShimServicesReader {

    private Map<String, Object> yamlData;

    /**
     * Constructs a ShimServicesReader instance and loads the YAML data from the
     * "shim_services.yaml" file located in the classpath.
     */
    public ShimServicesReader() {
        loadYaml();
    }

    /**
     * Loads the YAML data from the "shim_services.yaml" file.
     * This method reads the file and parses it into a Map structure.
     * If the file cannot be found or loaded, it throws a RuntimeException.
     */
    private void loadYaml() {
        Yaml yaml = new Yaml();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("shim_services.yaml")) {
            if (inputStream == null) {
                throw new RuntimeException("Could not find shim_services.yaml in resources");
            }
            yamlData = yaml.load(inputStream);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load shim_services.yaml", e);
        }
    }

    /**
     * Retrieves a list of service names defined in the YAML file.
     * Each service can be either a string or a map, and this method
     * extracts the names accordingly.
     *
     * @return a List of service names as Strings.
     */
    public List<String> getServiceNames() {
        List<Object> services = (List<Object>) yamlData.get("services");
        List<String> serviceNames = new ArrayList<>();
        for (Object service : services) {
            if (service instanceof String) {
                serviceNames.add((String) service);
            } else if (service instanceof Map) {
                serviceNames.addAll(((Map<String, Object>) service).keySet());
            }
        }
        return serviceNames;
    }

    /**
     * Retrieves a list of HDFS schemas defined in the YAML file.
     * It looks for the "hdfs" key within the services and returns
     * the associated schemas.
     *
     * @return a List of HDFS schema names as Strings.
     */
    public List<String> getHdfsSchemas() {
        List<Object> services = (List<Object>) yamlData.get("services");
        for (Object service : services) {
            if (service instanceof Map && ((Map<String, Object>) service).containsKey("hdfs")) {
                Map<String, Object> hdfs = (Map<String, Object>) ((Map<String, Object>) service).get("hdfs");
                return (List<String>) hdfs.get("schemas");
            }
        }
        return new ArrayList<>();
    }

    /**
     * Retrieves a list of options for a specific service defined in the YAML file.
     * It searches for the service by name and returns the associated options.
     *
     * @param serviceName the name of the service for which to retrieve options.
     * @return a List of options as Strings, or an empty list if the service is not found.
     */
    public List<String> getServiceOptions( String serviceName) {
        List<Object> services = (List<Object>) yamlData.get("services");
        for (Object service : services) {
            if (service instanceof Map && ((Map<String, Object>) service).containsKey(serviceName)) {
                Map<String, Object> hdfs = (Map<String, Object>) ((Map<String, Object>) service).get(serviceName);
                return (List<String>) hdfs.get("options");
            }
        }
        return new ArrayList<>();
    }

    /**
     * Retrieves a list of Hive drivers defined in the YAML file.
     * It looks for the "hive" key within the services and returns
     * the associated drivers.
     *
     * @return a List of Hive driver names as Strings.
     */
    public List<String> getHiveDrivers() {
        List<Object> services = (List<Object>) yamlData.get("services");
        for (Object service : services) {
            if (service instanceof Map && ((Map<String, Object>) service).containsKey("hive")) {
                Map<String, Object> hive = (Map<String, Object>) ((Map<String, Object>) service).get("hive");
                return (List<String>) hive.get("drivers");
            }
        }
        return new ArrayList<>();
    }
}
