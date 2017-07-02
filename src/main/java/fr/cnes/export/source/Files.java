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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.restlet.engine.Engine;

/**
 *
 * @author Jean-Christophe Malapert <jean-christophe.malapert@cnes.fr>
 */
public class Files implements IFiles {

    private final String url;   
    private final List<BrowseDirectory> deepBrowse = new ArrayList<>();    
    private int currentDeep = 0;

    private Files(String url) throws URISyntaxException, IOException {
        Engine.setLogLevel(Level.OFF);
        Engine.setRestletLogLevel(Level.OFF);
        this.url = url;       
        this.deepBrowse.add(new BrowseDirectory());
        this.deepBrowse.add(new BrowseDirectory());
    }
    
    public static IFiles openDirectory(String url) throws Exception {
        try {
            return new Files(url);
        } catch (URISyntaxException | IOException ex) {
            Logger.getLogger(Files.class.getName()).log(Level.SEVERE, null, ex);
            throw new Exception(ex);
        }
    }

    @Override
    public String nextFile() {
        try {
            readDirectory("");
        } catch (URISyntaxException | IOException ex) {
            Logger.getLogger(Files.class.getName()).log(Level.SEVERE, null, ex);
        }
        return (this.deepBrowse.get(0).getName() == null ) ? null : this.url+this.deepBrowse.get(0).getName() + this.deepBrowse.get(1).getName();
    }

    private String getName(String[] record) {
        return (record == null) ? null : record[record.length - 1];
    }

    private void readDirectory(String fragment) throws URISyntaxException, IOException {
        BrowseDirectory browse = this.deepBrowse.get(currentDeep);
        if(browse.getStatus() == BrowseDirectoryStatus.START) {
            browse.setDirectory(new FtpDirectory(this.url+fragment));
            browse.setStatus(BrowseDirectoryStatus.RUNNING);
            readDirectory(fragment);
        } else if(browse.getStatus() == BrowseDirectoryStatus.RUNNING) {
            String nextRecord = getName(browse.getDirectory().getNextRecord());
            if (nextRecord == null) {
                browse.setStatus(BrowseDirectoryStatus.END);
                browse.setName(nextRecord);                
                currentDeep = 0;
                readDirectory("");
            } else if(currentDeep == 1) {
                browse.setName(nextRecord);
            } else {                
                browse.setName((currentDeep == 0) ? nextRecord+"/" : nextRecord);
                currentDeep = 1;
                deepBrowse.get(currentDeep).setStatus(BrowseDirectoryStatus.START);
                readDirectory(browse.getName());                
            }
        } 
        
    }

}
