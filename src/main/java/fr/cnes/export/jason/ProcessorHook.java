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

import java.util.Queue;
import org.apache.log4j.Logger;

/**
 *
 * @author Jean-Christophe Malapert (jean-christophe.malapert@cnes.fr)
 */
public class ProcessorHook extends Thread {
    
    private static final Logger LOGGER = Logger.getLogger(ProcessorHook.class.getName());    
    
    private final Queue<String> dataQueue;
    
    public ProcessorHook(final Queue<String> dataQueue) {
        super();
        this.dataQueue = dataQueue;
    }
    
    @Override
    public void run(){
        if(!this.dataQueue.isEmpty()) {
            LOGGER.info("interrupt received, killing program"); 
        }         
    }    
}
