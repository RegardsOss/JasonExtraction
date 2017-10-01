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
package fr.cnes.export.settings;

/**
 * Consts contains all configuration variables, which are possible to use
 * in the config.properties.
 * @author Jean-Christophe Malapert (jean-christophe.malapert@cnes.fr)
 */
public class Consts {
    
    /**
     * Application's name.
     */
    public static final String APP_NAME = "Starter.APP_NAME";
    
    /**
     * Application's version.
     */
    public static final String VERSION = "Starter.VERSION";
    
    /**
     * Application's copyright.
     */
    public static final String COPYRIGHT = "Starter.COPYRIGHT";    
    
    /**
     * The output directory.
     */
    public static final String OUTPUT = "Starter.output";

    /**
     * The data source where data is taken.
     */
    public static final String FTP_DIRECTORY = "Starter.ftp_directory";
    
    /**
     * Displays the Geojson in a pretty way.
     */
    public static final String PRETTY_DISPLAY = "Starter.pretty_display";
}
