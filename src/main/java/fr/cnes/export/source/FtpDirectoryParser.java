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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author Jean-Christophe Malapert (jean-christophe.malapert@cnes.fr)
 */
public class FtpDirectoryParser {

    private static final String PATTERN_REGEXP = "(\\S+)\\s+(\\S+)\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+\\s+\\S+\\s+\\S+)\\s+(\\S+)";
    private static final Pattern PATTERN = Pattern.compile(PATTERN_REGEXP);

    private final BufferedReader br;
    private final String[] cols = new String[4]; 
    private int totalLine = 0;
    
    private static final Logger LOGGER = Logger.getLogger(FtpDirectoryParser.class.getName());

    public FtpDirectoryParser(final Reader reader) throws IOException {
        this.br = new BufferedReader(reader); 
        String line = this.br.readLine(); // total       
        this.totalLine = Integer.valueOf(line.split(" ")[1]);
        this.totalLine--;  
        line = this.br.readLine(); // .
        this.totalLine--;  
        line = this.br.readLine(); // ..
        this.totalLine--;         
    }
    
    public int getTotalLine() {
        return this.totalLine;
    }

    public String[] getNextLine() {
        String readLine = null;
        try {
            readLine = this.br.readLine();
        } catch (IOException ex) {
            LOGGER.log(Level.FATAL, null, ex);           
        }
        return (readLine != null) ? parseLine(readLine) : null;
    }

    private String[] parseLine(final String line) {
        Matcher m = PATTERN.matcher(line);
        boolean b = m.matches();
        if (b) {
            for (int i = 1; i <= m.groupCount(); i++) {
                 cols[i-1] = m.group(i);
            }
        }         
        return b ? cols : null;
    }
    
    public boolean isDirectory() {
        if(cols == null) {
            throw new RuntimeException();
        }
        return cols[0].startsWith("d");
    }
    
    public boolean isFile() {
        if(cols == null) {
            throw new RuntimeException();
        } 
        return !cols[0].startsWith("d");
    }    
    
    public void close() {
        try {
            this.br.close();
        } catch (IOException ex) {
            LOGGER.log(Level.FATAL, null, ex);
        }
    }

}
