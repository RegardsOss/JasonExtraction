 /******************************************************************************
 * Copyright 2017 CNES - CENTRE NATIONAL d'ETUDES SPATIALES
 *
 * This file is part of Regards.
 *
 * Regards is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Regards is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Regards.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package fr.cnes.export.jason;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.Variable;
import ucar.nc2.dataset.CoordinateAxis1DTime;
import ucar.nc2.dataset.VariableDS;
import ucar.nc2.time.CalendarDate;

/**
 *
 * @author Jean-Christophe Malapert <jean-christophe.malapert@cnes.fr>
 */
public class Metadata {
    
    private NetcdfDataset ncfile = null;
    private String uri;
    private final List<String> keywordsToExtract;
    private final Map<String, Object> data = new HashMap<>();
    private final Map<String, String> units = new HashMap<>();
    private final Map<String, String> description = new HashMap<>();
    private final Map<String, Map<Integer, String>> mappings = new HashMap<>();
    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(JASON.class.getName());    
    

    /**
     * Creates a Metedata thats contains the whished variables from a NetCdf file
     * @param keywordsToExtract variables to extract from NetCdf
     */
    public Metadata(final List<String> keywordsToExtract) {
        LOGGER.trace("Entering in Metadata");                                
        LOGGER.debug(keywordsToExtract);
        this.keywordsToExtract = keywordsToExtract;
        LOGGER.trace("Exiting in Metadata");        
    }

    /**
     * Adds semantic meanings for converting some variable values to human understandable value 
     * @param keyword variable
     * @param mapping mapping value/human understandable value
     */
    public void addMapping(final String keyword, final Map<Integer, String> mapping) {
        LOGGER.trace("Entering in addMapping");        
        LOGGER.debug("Add a mapping for "+keyword);
        this.mappings.put(keyword, mapping);
        LOGGER.trace("Exiting in addMapping");        
    }

    /**
     * Process NetCdf file.
     * @param uri location of the file
     * @throws URISyntaxException
     * @throws Exception 
     */
    public void process(final String uri) throws URISyntaxException, Exception {
        LOGGER.trace("Entering in process");        
        LOGGER.debug("Processing "+uri);        
        this.uri = uri;
        try {
            NetcdfFile file = NetcdfFile.openInMemory(new URI(uri));
            ncfile = new NetcdfDataset(file);
            extractVariablesFromNetCdf(keywordsToExtract, ncfile, data, units, description);
        } catch (IOException ioe) {
            if (null != ncfile) {
                try {
                    LOGGER.debug("Closing the file", ioe);        
                    ncfile.close();
                } catch (IOException ex) {
                    LOGGER.error("Unable to close the NetCdf file", ex);                            
                    throw new Exception(ex);
                }
            }
        }
        LOGGER.trace("Exiting in process");                
    }

    /**
     * Returns the values of the variable.
     * Null is returned whether the variable is not found
     * @param <T> String[] or Array
     * @param keyword variable to get
     * @return the values
     */
    public <T> T getData(final String keyword) {
        return (T) this.data.getOrDefault(keyword, null);
    }

    /**
     * Returns the unit of the variable.
     * Null is returned whether the variable is not found
     * @param keyword variable
     * @return the unit
     */
    public String getUnit(final String keyword) {
        return this.units.getOrDefault(keyword, null);
    }

    /**
     * Returns the description of the variable.
     * Null is returned whether the variable is not found
     * @param keyword variable
     * @return the description
     */
    public String getDescription(final String keyword) {
        return this.description.getOrDefault(keyword, null);
    }

    /**
     * Tests if the variable has a semantic mapping.
     * @param keyword variable
     * @return True when a mapping has been done otherwise False
     */
    public boolean hasMapping(final String keyword) {
        return this.mappings.containsKey(keyword);
    }

    /**
     * Returns the mapping for the variable.
     * @param keyword variable
     * @return the mapping
     */
    public Map<Integer, String> getMapping(final String keyword) {
        return this.mappings.getOrDefault(keyword, null);
    }

    /**
     * Returns the global metadata of the NetCdf file.
     * @return the global metadata
     */
    public Map<String, Object> getGlobalMetadata() {
        LOGGER.trace("Entering in getGlobalMetadata");                
        Map metadata = new HashMap<>();
        List<Attribute> attributes = this.ncfile.getGlobalAttributes();
        attributes.stream().forEach((attribute) -> {
            String keyword = attribute.getShortName();
            Object value = (attribute.getNumericValue() == null)
                    ? attribute.getStringValue() : attribute.getNumericValue();
            metadata.put(keyword, value);
        });
        LOGGER.debug(metadata);
        LOGGER.trace("Exiting in getGlobalMetadata");                        
        return metadata;
    }

    /**
     * Extacts time variable and converts the value as a date.
     * @param keywords variable
     * @param data stored result
     */
    private void extractTime(final List<String> keywords, final Map<String, Object> data) {
        LOGGER.trace("Entering in extractTime");                        
        if (keywords.contains("time")) {
            try {
                Variable time = ncfile.findVariable("time");
                CoordinateAxis1DTime axis = CoordinateAxis1DTime.factory(ncfile, (VariableDS) time, new Formatter());
                int length = time.getElementSize();
                String[] times = new String[length];
                for (int i = 0; i < length; i++) {
                    CalendarDate date = axis.getCalendarDate(i);
                    times[i] = date.toString();
                }
                data.put("time", times);
            } catch (IOException ex) {
                LOGGER.debug("Unable to extract time", ex);                                        
            }
        }
        LOGGER.trace("Exiting in extractTime");                                
    }

    /**
     * Extracts a variable.
     * @param keyword variable
     * @param dataVariables stored result
     * @param unitsVariables stored unit of the variable
     * @param descriptionVariables stored description of the variable
     */
    private void extractVariable(final String keyword, final Map<String, Object> dataVariables, Map<String, String> unitsVariables, Map<String, String> descriptionVariables) {
        LOGGER.trace("Entering in extractVariable");                                
        LOGGER.debug("Extracting variable "+keyword);
        Variable variable = getVariable(keyword);
        if (variable != null) {
            Array values = this.getDataFromVariable(keyword);
            dataVariables.put(keyword, values);

            Attribute unitsAttribute = variable.findAttributeIgnoreCase("units");
            if (unitsAttribute != null) {
                unitsVariables.put(keyword, unitsAttribute.getStringValue());
            }

            String desc = variable.getDescription();
            if (desc != null) {
                descriptionVariables.put(keyword, desc);
            }
        }
        LOGGER.trace("Exiting in extractVariable");                                        
    }

    /**
     * Extracts all whished variables from NetCdf.
     * @param keywords keywords to extract
     * @param ncDs NetCdf dataset
     * @param dataVariable stored result
     * @param unitsVariable stored unit of the variable
     * @param descriptionVariable  stored description of the variable
     */
    protected void extractVariablesFromNetCdf(final List<String> keywords, final NetcdfDataset ncDs, Map<String, Object> dataVariable, Map<String, String> unitsVariable, Map<String, String> descriptionVariable) {
        LOGGER.trace("Entering in extractVariablesFromNetCdf");                                        
        dataVariable.clear();               
        extractTime(keywords, dataVariable);
        List<String> keywordsToProcess = new ArrayList<>();
        keywordsToProcess.addAll(keywords);
        keywordsToProcess.remove("time");
        keywords.stream().forEach((keyword) -> {
            extractVariable(keyword, dataVariable, unitsVariable, descriptionVariable);
        });
        LOGGER.trace("Exiting in extractVariablesFromNetCdf");                                                
    }

    /**
     * Returns the variable from NetCdf.
     * @param name variable name
     * @return the variable from NetCdf
     */
    protected Variable getVariable(final String name) {
        return ncfile.findVariable(name);
    }

    /**
     * Returns the values of the variable
     * @param name variable name
     * @return the values
     */
    protected Array getDataFromVariable(final String name) {
        LOGGER.trace("Entering in getDataFromVariable");                                                
        Array result;
        try {
            result = getVariable(name).read();
        } catch (IOException ex) {
            LOGGER.error("Unable to read the variable "+name);                                                            
            result = null;
        }
        LOGGER.trace("Exiting in getDataFromVariable");                                                        
        return result;
    }

    /**
     * Returns the data type of the variable
     * @param name variable
     * @return the datatype
     */
    protected DataType getDataType(final String name) {
        return getVariable(name).getDataType();
    }

}
