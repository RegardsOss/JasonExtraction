/**
 * ****************************************************************************
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
 * ****************************************************************************
 */
package fr.cnes.export.jason;

import fr.cnes.export.settings.Consts;
import fr.cnes.export.settings.Settings;
import fr.cnes.export.source.Files;
import fr.cnes.export.source.IFiles;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.restlet.data.LocalReference;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;

/**
 * Main class to convert a part of the NCDF files from JASON mission to GeoJson.
 * @author Jean-Christophe Malapert (jean-christophe.malapert@cnes.fr)
 */
public class JASON {

    static {
        ClientResource client = new ClientResource(LocalReference.createClapReference("class/config.properties"));
        Representation configurationFile = client.get();
        try {
            Settings.getInstance().setPropertiesFile(configurationFile.getStream());
        } catch (IOException ex) {
            java.util.logging.Logger.getAnonymousLogger().severe(ex.getMessage());
        }

        client = new ClientResource(LocalReference.createClapReference("class/log4.properties"));
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
    private static String FTP_DIRECTORY = "TBD";

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
    private final Queue<String> dataQueue = new ConcurrentLinkedQueue<>();

    /**
     * Constructor.
     */
    public JASON() {
        LOGGER.info("Starting Jason program.");
    }
    
    public JASON(final String FtpDir) {
        LOGGER.info("Starting Jason program.");
	this.FTP_DIRECTORY = FtpDir;
    }

    /**
     * Processes the process to transform a part of the NETCDF to GeoJSon.
     */
    public void processConversion() {
	final Settings settings = Settings.getInstance();
        LOGGER.trace("Entering in processConvertion");
	if (this.FTP_DIRECTORY == "TBD") {
		this.FTP_DIRECTORY = settings.getString(Consts.FTP_DIRECTORY);
	}
	LOGGER.trace(String.format("FTP_DIRECTORY : %s", this.FTP_DIRECTORY));
        try {
            IFiles fileIterator = Files.openDirectory(this.FTP_DIRECTORY);
            final Map<String, Object> attributes = initProcessingAttributes();
            countFilesToProcess(fileIterator, attributes, dataQueue);
            waitDataQueueContainsOneRecord(dataQueue);
            processFilesInQueue(startTime, dataQueue, attributes);
        } catch (Exception ex) {
            LOGGER.error(String.format("Cannot process %s", this.FTP_DIRECTORY), ex);
        }
        LOGGER.trace("exiting in processConvertion");
    }

    /**
     * Starts the conversion.
     */
    public void start() {
        LOGGER.trace("Entering in start");

        /**
         * Starts the conversion.
         */
        final Thread client = new Thread() {
            @Override
            public void run() {
                processConversion();
            }
        };

        /**
         * Intercepts kill and shutdown event.
         */
        Runtime.getRuntime().addShutdownHook(new ProcessorHook(dataQueue));

        LOGGER.info("Starts the client");
        client.start();
        LOGGER.trace("Exiting in start");
    }

    /**
     * Displays help.
     */
    private static void displayHelp() {
        Settings settings = Settings.getInstance();
        StringBuilder help = new StringBuilder();
        help.append("------------ Help for DOI Server -----------\n");
        help.append("\n");
        help.append("Usage: java -jar ").append(settings.getString(Consts.APP_NAME)).append("-").append(settings.getString(Consts.VERSION)).append(".jar [OPTIONS]\n");
        help.append("\n\n");
        help.append("with OPTIONS:\n");
        help.append("  -h|--help                    : This output\n");
        help.append("  -d                           : Displays the configuration file\n");
        help.append("  -f <path>                    : Loads the configuation file\n");
        help.append("  -u <ftppath>                 : Specify the root URL to proceed\n");
        help.append("  -v|--version                 : DOI server version\n");
        help.append("\n");
        help.append("\n");
        System.out.println(help.toString());
    }

    /**
     * Displays version.
     */
    private static void displayVersion() {
        final Settings settings = Settings.getInstance();
        final String appName = settings.getString(Consts.APP_NAME);
        final String version = settings.getString(Consts.VERSION);
        final String copyright = settings.getString(Consts.COPYRIGHT);
        System.out.println(appName + " (" + copyright + ") - Version:" + version + "\n");
    }

    /**
     * Main
     * @param argv command line arguments
     * @throws URISyntaxException
     * @throws IOException
     * @throws Exception 
     */
    public static void main(final String[] argv) throws URISyntaxException, IOException, Exception {
        final Settings settings = Settings.getInstance();
        final String progName = "java -jar " + settings.getString(Consts.APP_NAME) + "-" + settings.getString(Consts.VERSION) + ".jar";

        int c;
        String arg;

        boolean hasOwnProperties = false;
        boolean persoURL = false;
        String persoURLval = " ";
        LongOpt[] longopts = new LongOpt[2];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[1] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'v');

        // 
        Getopt g = new Getopt(progName, argv, "hvdfu:", longopts);
        //        
        while ((c = g.getopt()) != -1) {
            switch (c) {
                case 'h':
                    displayHelp();
                    break;
                //    
                case 'd':
                    settings.displayConfigFile();
                    break;
                case 'f':
                    arg = g.getOptarg();
                     {
                        try {
                            settings.setPropertiesFile(arg);
                            hasOwnProperties = true;
                        } catch (IOException ex) {
                            LOGGER.info(ex.getMessage());
                        }
                    }
                    break;
                case 'u':
                    persoURLval = g.getOptarg();
		    persoURL = true;
                    break;
                case 'v':
                    displayVersion();
                    break;
                case '?':
                    break; // getopt() already printed an error
                default:
                    System.err.println(String.format("getopt() returned {0}\n", c));

            }
        }

        for (int i = g.getOptind(); i < argv.length; i++) {
            System.err.println(String.format("Non option argv element: {0}\n", argv[i]));
        }

	if (persoURL) {
		    JASON jason = new JASON(persoURLval);
                    jason.start();

	} else {
        	if (argv.length == 0 || hasOwnProperties) {
        	    JASON jason = new JASON();
        	    jason.start();
        	}
	}

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
     * @return map to monitor the processing
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
     * Count files to processConversion.
     *
     * @param fileIterator File iterator
     * @param attributes attributes for the processing
     * @param dataQueue Files to processConversion queue
     */
    private void countFilesToProcess(final IFiles fileIterator, final Map<String, Object> attributes, final Queue<String> dataQueue) {
        LOGGER.trace("Entering in countFilesToProcess");
        Thread t = new Thread() {
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
                LOGGER.info(i + " files to process");
            }

            private void displayMessage(int i) {
                System.out.print("\r Indexing " + i + " files");
            }
        };
        t.setUncaughtExceptionHandler((Thread myThread, Throwable ex) -> {
            LOGGER.log(Level.ERROR, "A not recoverable error has been detected during the indexation, the program is shutting down", ex);
            System.err.println("A not recoverable error has been detected during the indexation, the program is shutting down. Please look at the log file");
            dataQueue.clear();
            Thread.currentThread().interrupt();            
        });
        t.start();
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
    private ExecutorService processFilesInQueue(long startTime, final Queue<String> dataQueue, final Map<String, Object> attributes) throws InterruptedException {
        LOGGER.trace("Entering in processFilesInQueue");
        ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            es.execute(new Processor(startTime, attributes, dataQueue));
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.MINUTES);
        LOGGER.trace("Exiting in processFilesInQueue");
        return es;
    }

}
