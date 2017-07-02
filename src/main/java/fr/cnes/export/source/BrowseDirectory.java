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
package fr.cnes.export.source;

/**
 *
 * @author Jean-Christophe Malapert <jean-christophe.malapert@cnes.fr>
 */
public class BrowseDirectory {
    
    public BrowseDirectoryStatus status;
    public String name;
    public Directory directory;
    
    public BrowseDirectory() {
        setStatus(BrowseDirectoryStatus.START);
    }

    /**
     * @return the status
     */
    public BrowseDirectoryStatus getStatus() {
        return status;
    }

    /**
     * @param status the status to set
     */
    public void setStatus(BrowseDirectoryStatus status) {
        this.status = status;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the directory
     */
    public Directory getDirectory() {
        return directory;
    }

    /**
     * @param directory the directory to set
     */
    public void setDirectory(Directory directory) {
        this.directory = directory;
    }
    
}
