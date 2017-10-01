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

import java.util.Map;
import java.util.Queue;
import ucar.ma2.Array;
import static fr.cnes.export.jason.JASON.KEYWORDS_TO_EXTRACT;
import static fr.cnes.export.jason.JASON.SURFACE_TYPE_MAPPING;
import fr.cnes.export.settings.Consts;
import fr.cnes.export.settings.Settings;
import fr.cnes.geojson.GeoJsonParser;
import fr.cnes.geojson.GeoJsonWriter;
import fr.cnes.geojson.geometry.LineString;
import fr.cnes.geojson.object.Feature;
import fr.cnes.geojson.object.FeatureCollection;
import fr.cnes.geojson.object.Geometry;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import org.apache.log4j.Logger;

/**
 * Process a file.
 *
 * @author Jean-Christophe Malapert <jean-christophe.malapert@cnes.fr>
 */
public class Processor implements Runnable {

    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getLogger(Processor.class.getName());

    /**
     * List of URI to process.
     */
    private final Queue<String> dataQueue;

    /**
     * List of variables to extract.
     */
    private final Map<String, Object> attributes;

    /**
     * Extracted metadata from the NetCDF file.
     */
    private final Metadata metadata;

    /**
     * Start time of the global process.
     */
    private final long startTime;

    /**
     * Init the GeoJson writer library.
     */
    private final GeoJsonWriter writer = new GeoJsonWriter();

    /**
     * Init the GeoJson parser library.
     */
    private final GeoJsonParser parser = new GeoJsonParser();

    /**
     * Constructs the global process.
     *
     * @param startTime start time
     * @param attributes attributes to extract
     * @param dataQueue List of files to process
     */
    public Processor(final long startTime, final Map<String, Object> attributes,
            final Queue<String> dataQueue) {
        final Settings settings = Settings.getInstance();
        this.startTime = startTime;
        this.attributes = attributes;
        this.dataQueue = dataQueue;
        this.metadata = new Metadata(KEYWORDS_TO_EXTRACT);
        this.metadata.addMapping("surface_type", SURFACE_TYPE_MAPPING);
        this.writer.getOptions().put(GeoJsonWriter.FIX_LONGITUDE, true);
        final String prettyDisp = settings.getString(Consts.PRETTY_DISPLAY, "false");                
        LOGGER.trace("Set pretty display to "+prettyDisp);        
        this.writer.getOptions().put(GeoJsonWriter.PRETTY_DISPLAY, Boolean.parseBoolean(prettyDisp));
    }

    /**
     * Extracts the metadata from the file and transforms it to GeoJson.
     *
     * @param uri file
     * @throws URISyntaxException
     * @throws InterruptedException
     * @throws IOException
     */
    private void process(final String uri) throws URISyntaxException, InterruptedException, IOException {
        LOGGER.trace("Entering in process with argument " + uri);        
        this.metadata.process(uri);
        save(this.metadata, uri);
        LOGGER.trace("Exiting in process");                
    }

    /**
     * Gets the output filename on the disk based on the URI of the file
     *
     * @param uri file
     * @return GeoJson filename
     */
    private static File getFilenameUriAsGeoJson(final String uri) {
        LOGGER.trace("Entering in getFilenameUriAsGeoJson with argument " + uri);
        final String fileName = uri.substring(uri.lastIndexOf('/') + 1, uri.length());
        final String fileNameWithoutExtension = fileName.substring(0, fileName.lastIndexOf('.'));
        final String outputDir = Settings.getInstance().getString(Consts.OUTPUT);
        final StringBuilder fileBuilder = new StringBuilder();
        fileBuilder.append(outputDir);
        fileBuilder.append(fileNameWithoutExtension);
        fileBuilder.append(".geojson");
        LOGGER.trace("Exiting in getFilenameUriAsGeoJson with result " + fileBuilder.toString());
        return new File(fileBuilder.toString());
    }

    /**
     * Tests if the file has already been processed and not corrupted.
     *
     * @param uri file
     * @return True when the uri has already been processed otherwise False
     */
    private boolean isUriAlreadyProcessedAndValid(final String uri) {
        LOGGER.trace("Entering in isUriAlreadyProcessedAndValid with argument " + uri);
        boolean result;
        final File file = getFilenameUriAsGeoJson(uri);
        result = file.exists();
        LOGGER.trace("File "+file.getName()+" exists ? "+result);
        if (result) {
            try {
                this.parser.parse(file);               
            } catch (IOException ex) {
                LOGGER.info(file.getName() + " is corrupted, add it in the queue", ex);
                boolean added = this.dataQueue.add(uri);
                if (!added) {
                    LOGGER.error("Cannot add the corrupted file " + file.getName() + " in the queue");
                }
                result = false;
            }
        }
        LOGGER.trace("Exiting in isUriAlreadyProcessedAndValid with result " + result);
        return result;
    }

    @Override
    public void run() {
        while (!this.dataQueue.isEmpty()) {
            String uri = this.dataQueue.poll();

            final long startProcessing = System.currentTimeMillis();
            LOGGER.info(String.format("Starting the processing of %s", uri));
            try {
                if (isUriAlreadyProcessedAndValid(uri)) {
                    LOGGER.info(String.format("Skip existing uri %s on disk", uri));
                    Thread.sleep(100);
                    continue;
                }
                process(uri);
                synchronized (this.attributes) {
                    int nbFiles = (Integer) this.attributes.get("nbFiles");
                    nbFiles++;
                    this.attributes.put("nbFiles", nbFiles);
                }

                progressPercentage(this.startTime, (boolean) this.attributes.get("isCounted"), (int) this.attributes.get("nbFiles"), (int) this.attributes.get("nbTotalFiles"));
                final long endProcessing = System.currentTimeMillis();
                final float timeProcessing = (float) ((endProcessing - startProcessing) / 1000.0f);
                LOGGER.info("processed file in " + timeProcessing + " s");
            } catch (URISyntaxException | InterruptedException | IOException ex) {
                final long endProcessing = System.currentTimeMillis();
                final float timeProcessing = (float) ((endProcessing - startProcessing) / 1000.0f);
                LOGGER.error(String.format("Unable to process the file %s - %s s", uri, timeProcessing), ex);
            }
        }
    }

    /**
     * Process the variable to extract.
     *
     * @param metadata extracted metadata from the file
     * @param keywords keywords to extract
     * @return GeoJSon node
     */
    private Map<String, Object> getVariables(Metadata metadata, List<String> keywords) {
        final Map<String, Object> variables = new HashMap<>();
        keywords.stream().filter((keyword) -> !(keyword.equals("lon") || keyword.equals("lat"))).forEach((keyword) -> {
            storeVariable(metadata.getData(keyword), variables, keyword);
        });
        return variables;
    }

    /**
     * Stores the variable as an array according to its datatype.
     *
     * @param valueObj value of the variable
     * @param variables list of variables to transform to GeoJson
     * @param keyword keyword of the variable
     */
    private void storeVariable(Object valueObj, Map<String, Object> variables, String keyword) {
        if (valueObj == null) {
            return;
        }
        if (valueObj instanceof Array) {
            final Array val = (Array) valueObj;
            final String datatype = val.getElementType().getCanonicalName();
            switch (datatype) {
                case "double":
                    final double[] doubleValues = (double[]) val.copyTo1DJavaArray();
                    variables.put(keyword, doubleValues);
                    break;
                case "byte":
                    final byte[] byteValues = (byte[]) val.copyTo1DJavaArray();
                    final String[] values = new String[byteValues.length];
                    final Map<Integer, String> mapping = metadata.getMapping(keyword);
                    for (int i = 0; i < byteValues.length; i++) {
                        final Byte byteVal = byteValues[i];
                        final int valInt = byteVal.intValue();
                        final String desc;
                        if (mapping == null) {
                            desc = String.valueOf(valInt);
                        } else {
                            desc = mapping.get(valInt);
                        }
                        values[i] = desc;
                    }
                    variables.put(keyword, values);
                    break;
                default:
                    break;
            }
        } else if (valueObj instanceof String[]) {
            final String[] times = (String[]) valueObj;
            variables.put(keyword, times);
        }
    }

    /**
     * Stores the uri of the file in a specific geojson node. This geojson node
     * is used so that Mizar represents this information as a file to download.
     *
     * @param uri file
     * @return GeoJson node
     */
    private Map<String, Object> getServices(final String uri) {
        final Map<String, Object> download = new HashMap<>();
        download.put("mimetype", "application/x-netcdf");
        download.put("url", uri);
        final Map<String, Object> services = new HashMap<>();
        services.put("download", download);
        return services;
    }

    /**
     * Saves the file as a GeoJson file.
     *
     * @param metadata extracted variables
     * @param uri file
     * @throws URISyntaxException
     * @throws FileNotFoundException
     * @throws IOException
     */
    private void save(final Metadata metadata, final String uri) throws URISyntaxException, FileNotFoundException, IOException {
        final String fileName = uri.substring(uri.lastIndexOf('/') + 1, uri.length());
        final Feature feature = writer.createFeature();
        feature.setId(fileName);
        feature.setGeometry(createGeometry(metadata));
        feature.getProperties().putAll(metadata.getGlobalMetadata());
        feature.getProperties().put("variables", getVariables(metadata, KEYWORDS_TO_EXTRACT));
        feature.getForeignMembers().put("services", getServices(uri));
        final FeatureCollection fc = writer.createFeatureCollection();
        fc.getFeatures().add(feature);

        try (final FileOutputStream fos = new FileOutputStream(getFilenameUriAsGeoJson(uri))) {
            final String result = writer.toJson(fc);
            fos.write(result.getBytes());
        }
    }

    /**
     * Create the GeoJson geometry
     *
     * @param extVariable extracted metadata from the file
     * @return the GeoJson geometry
     */
    private Geometry createGeometry(Metadata extVariable) {
        final LineString geom = writer.createGeometry(LineString.class);
        final Array longitudeVariable = extVariable.getData("lon");
        final Array latitudeVariable = extVariable.getData("lat");
        final double[] longitudes = (double[]) longitudeVariable.copyTo1DJavaArray();
        final double[] latitudes = (double[]) latitudeVariable.copyTo1DJavaArray();
        final double[][] coordinates = new double[longitudes.length][];
        for (int i = 0; i < longitudes.length; i++) {
            coordinates[i] = new double[]{longitudes[i], latitudes[i]};
        }
        geom.setPoints(coordinates);
        return geom;
    }

    /**
     * Displays a progress bar.
     *
     * @param startTime start time
     * @param isFinishedToCount
     * @param remain number of remaining URI to process
     * @param total total number of URI to process
     */
    public static void progressPercentage(long startTime, boolean isFinishedToCount, int remain, int total) {
        if (isFinishedToCount) {
            final int maxBareSize = 10; // 10unit for 100%
            final int remainProcent = ((100 * remain) / total) / maxBareSize;
            final char defaultChar = '-';
            final String icon = "*";
            final String bare = new String(new char[maxBareSize]).replace('\0', defaultChar) + "]";
            final StringBuilder bareDone = new StringBuilder();
            bareDone.append("[");
            for (int i = 0; i < remainProcent; i++) {
                bareDone.append(icon);
            }
            final String bareRemain = bare.substring(remainProcent, bare.length());
            final long currentTime = System.currentTimeMillis();

            final long diffTimePerFile = (currentTime - startTime) / remain;
            final long estimatedTimeToFinish = (total - remain) * diffTimePerFile;
            final float estimatedTimeToFinishInMn = estimatedTimeToFinish / 60000.0f;
            final String time = String.format("%.2f", estimatedTimeToFinishInMn);
            System.out.print("\r" + bareDone + bareRemain + " " + remainProcent * 10 + "%" + "(" + remain + "/" + total + ")  - Still " + time + " mn");
            if (remain == total) {
                System.out.print("\n");
            }
        }
    }
}
