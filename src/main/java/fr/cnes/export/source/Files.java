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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.restlet.engine.Engine;
import org.apache.log4j.Logger;
import org.restlet.resource.ResourceException;


/**
 *
 * @author Jean-Christophe Malapert <jean-christophe.malapert@cnes.fr>
 */
public class Files implements IFiles {

    private final String url;   
    private final List<BrowseDirectory> deepBrowse = new ArrayList<>();    
    private int currentDeep = 0;
    
    private static final Logger LOGGER = Logger.getLogger(Files.class.getName());

    private Files(String url) throws URISyntaxException, IOException {
        Engine.setLogLevel(java.util.logging.Level.OFF);
        Engine.setRestletLogLevel(java.util.logging.Level.OFF);
        this.url = url;               
        this.deepBrowse.add(new BrowseDirectory());
    }
    
    public static IFiles openDirectory(String url) throws Exception {
        try {
            return new Files(url);
        } catch (URISyntaxException | IOException ex) {
            LOGGER.log(Level.FATAL, null, ex);
            throw new Exception(ex);
        }
    }

    @Override
    public String nextFile() {
        try {
            readDirectory("");
        } catch (URISyntaxException | IOException ex) {
            LOGGER.log(Level.FATAL, null, ex);            
        }
        String file  = (this.deepBrowse.isEmpty() ) ? null : this.deepBrowse.get(currentDeep).getName();
        LOGGER.log(Level.INFO, String.format("Indexing file %s", file == null ? "is over" : file));        
        return file;
    }

    private String getName(String[] record) {
        return (record == null) ? null : record[record.length - 1];
    }

    private void readDirectory(String fragment) throws URISyntaxException, IOException, ResourceException {
        if(this.deepBrowse.size() < currentDeep+1) {
            this.deepBrowse.add(new BrowseDirectory());
        }
        BrowseDirectory browse = this.deepBrowse.get(currentDeep);
        if(null != browse.getStatus()) switch (browse.getStatus()) {
            case START:
                browse.setDirectory(new FtpDirectory(this.url+fragment));
                browse.setName(null);
                browse.setStatus(BrowseDirectoryStatus.RUNNING);
                LOGGER.log(Level.INFO, String.format("Start reading directory %s", fragment));                   
                readDirectory(fragment);
                break;
            case RUNNING:
                String[] nextRecord = browse.getDirectory().getNextRecord();
                if(nextRecord == null) {
                    browse.setName(null);
                    browse.setStatus(BrowseDirectoryStatus.END);
                    readDirectory("");
                } else if (browse.getDirectory().isDirectory()) {
                    String directoryName = getName(nextRecord)+"/";  
                    browse.setName(directoryName);                    
                    currentDeep++;
                    readDirectory(directoryName);
                } else {
                    String fileName = getName(nextRecord);
                    browse.setName(browse.getDirectory().getSourceDirectory()+fileName);
                }
                break;
            case END:  
                String parentDirectory = (currentDeep-1 >= 0) ? this.deepBrowse.get(currentDeep-1).getName() : "";
                LOGGER.log(Level.INFO, String.format("Finish reading directory %s", parentDirectory));
                browse.getDirectory().close();
                this.deepBrowse.remove(currentDeep);
                currentDeep--; 
                if(currentDeep >= 0) {
                    readDirectory("");
                }
                break;
            
        } else {
            LOGGER.log(Level.FATAL, String.format("getStatus is null for %s", fragment));            
        }      
    }
}
