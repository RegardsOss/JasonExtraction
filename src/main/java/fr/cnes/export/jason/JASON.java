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

import fr.cnes.export.source.Files;
import fr.cnes.export.source.IFiles;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.restlet.data.LocalReference;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;

/**
 *
 * @author Jean-Christophe Malapert <jean-christophe.malapert@cnes.fr>
 */
public class JASON {   
    
    static {
        ClientResource client = new ClientResource(LocalReference.createClapReference("class/log4.properties"));
        //InputStream is = Starter.class.getResourceAsStream("logging.properties");
        Representation logging = client.get();
        try {
            PropertyConfigurator.configure(logging.getStream());
        } catch (final IOException e) {
            java.util.logging.Logger.getAnonymousLogger().severe("Could not load default logging.properties file");
            java.util.logging.Logger.getAnonymousLogger().severe(e.getMessage());
        } finally {
            client.release();
        }
    }      

    private static final int THREAD_COUNT = 8;

    public static int processedFile = 0;

    public static final List<String> KEYWORDS_TO_EXTRACT = Arrays.asList(
            "time", "lon", "lat", "surface_type", "range_numval_ku", "range_rms_ku",
            "range_ku", "rad_wet_tropo_corr", "iono_corr_alt_ku",
            "sig0_ku", "wind_speed_alt", "off_nadir_angle_wf_ku", "sig0_numval_ku",
            "sig0_rms_ku");

    public static final Map<Integer, String> SURFACE_TYPE_MAPPING = new HashMap<Integer, String>() {
        private static final long serialVersionUID = 1L;

        {
            put(0, "open oceans or semi-enclosed seas");
            put(1, "enclosed seas or lakes");
            put(2, "continental ice");
            put(3, "land. See Jason-1 User Handbook");
        }
    };
    private static final Logger LOGGER = Logger.getLogger(JASON.class.getName());    
    private final long startTime = System.currentTimeMillis();
    private final Queue<String> dataQueue =  new ConcurrentLinkedQueue<>();

    public JASON() {
        LOGGER.info("Starting Jason program.");
    }

    public void processConvertion() {
        LOGGER.trace("Entering in processConvertion");
        final String FTP_DIRECTORY = "ftp://avisoftp.cnes.fr/AVISO/pub/jason-1/gdr_e/";
        try {
            IFiles fileIterator = Files.openDirectory(FTP_DIRECTORY);
            final Map<String, Object> attributes = initProcessingAttributes();
            countFilesToProcess(fileIterator, attributes, dataQueue);
            waitDataQueueContainsOneRecord(dataQueue);
            processFilesInQueue(startTime, dataQueue, attributes);
        } catch (Exception ex) {
            LOGGER.error(String.format("Cannot process %s", FTP_DIRECTORY), ex);
        }
        LOGGER.trace("exiting in processConvertion");
    }

    public void start() {
        LOGGER.trace("Entering in start");

        final Thread client = new Thread() {
            @Override
            public void run() {
                processConvertion();
            }
        };

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOGGER.info("interrupt received, killing server…");
            }
        });

        LOGGER.info("Starts the client");
        client.start();
        LOGGER.trace("Exiting in start");
    }

    public static void main(final String[] argv) throws URISyntaxException, IOException, Exception {
        JASON jason = new JASON();
        jason.start();
    }

    /**
     * Inits a map, which is used to store some attributes during the
     * processing. The stored attributes are :
     * <ul>
     * <li>isCounted</li>
     * <li>nbFiles</li>
     * <li>nbTotalFiles</li>
     * </ul>
     *
     * @return
     */
    private Map<String, Object> initProcessingAttributes() {
        LOGGER.trace("Entering in initProcessingAttributes");        
        final Map<String, Object> attributes = new ConcurrentHashMap<>();
        attributes.put("isCounted", false);
        attributes.put("nbFiles", 0);
        attributes.put("nbTotalFiles", 0);
        LOGGER.debug(attributes);        
        LOGGER.trace("Exiting in initProcessingAttributes");                
        return attributes;
    }

    /**
     * Count files to processConvertion.
     *
     * @param fileIterator File iterator
     * @param attributes attributes for the processing
     * @param dataQueue Files to processConvertion queue
     */
    private void countFilesToProcess(final IFiles fileIterator, final Map<String, Object> attributes, final Queue<String> dataQueue) {
        LOGGER.trace("Entering in countFilesToProcess");        
        new Thread() {
            @Override
            public void run() {
                String file;
                int i = 0;
                while ((file = fileIterator.nextFile()) != null) {
                    dataQueue.add(file);
                    i++;
                    displayMessage(i);
                }
                System.out.print("\n");
                attributes.put("isCounted", true);
                attributes.put("nbTotalFiles", i);
                LOGGER.debug(attributes);
                LOGGER.info(i+" files to process");                        
            }

            private void displayMessage(int i) {
                System.out.print("\r Indexing " + i + " files");
            }
        }.start();
        LOGGER.trace("Exiting in countFilesToProcess");                
    }

    /**
     * Waits the files queue starts to be filled.
     *
     * @param dataQueue files queue
     * @throws InterruptedException
     */
    private void waitDataQueueContainsOneRecord(final Queue<String> dataQueue) throws InterruptedException {
        LOGGER.trace("Entering in waitDataQueueContainsOneRecord");                
        while (dataQueue.isEmpty()) {
            // Wait for the thread to start writing into the queue
            LOGGER.debug("Waiting 10s");                            
            Thread.sleep(10);
        }
        LOGGER.trace("Exiting in waitDataQueueContainsOneRecord");                        
    }

    /**
     * Process files from the queue
     *
     * @param startTime start time of the program
     * @param dataQueue files queue
     * @param attributes attributes for processing
     * @throws InterruptedException
     */
    private void processFilesInQueue(long startTime, final Queue<String> dataQueue, final Map<String, Object> attributes) throws InterruptedException {
        LOGGER.trace("Entering in processFilesInQueue");                        
        ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            es.execute(new Processor(startTime, attributes, dataQueue));
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.MINUTES);
        LOGGER.trace("Exiting in processFilesInQueue");                                
    }

}
