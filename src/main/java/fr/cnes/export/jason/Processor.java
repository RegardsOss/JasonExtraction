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

import java.util.Map;
import java.util.Queue;
import ucar.ma2.Array;
import static fr.cnes.export.jason.JASON.KEYWORDS_TO_EXTRACT;
import static fr.cnes.export.jason.JASON.SURFACE_TYPE_MAPPING;
import fr.cnes.export.settings.Consts;
import fr.cnes.export.settings.Settings;
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
import org.apache.log4j.Logger;

/**
 * Process a file.
 *
 * @author Jean-Christophe Malapert <jean-christophe.malapert@cnes.fr>
 */
public class Processor implements Runnable {     

    private final Queue<String> dataQueue;
    private final Map<String, Object> attributes;
    private final Metadata metadata;
    private final long startTime;
    private final GeoJsonWriter writer = new GeoJsonWriter();
    private static final Logger LOGGER = Logger.getLogger(Processor.class.getName());    


    public Processor(final long startTime, final Map<String, Object> attributes, final Queue<String> dataQueue) {        
        this.startTime = startTime;
        this.attributes = attributes;
        this.dataQueue = dataQueue;
        this.metadata = new Metadata(KEYWORDS_TO_EXTRACT);
        this.metadata.addMapping("surface_type", SURFACE_TYPE_MAPPING);
        this.writer.getOptions().put(GeoJsonWriter.FIX_LONGITUDE, true);
        this.writer.getOptions().put(GeoJsonWriter.PRETTY_DISPLAY, false);
    }

    @Override
    public void run() {
        while (!this.dataQueue.isEmpty()) {
            String uri = this.dataQueue.poll();
            final long startProcessing = System.currentTimeMillis();
            LOGGER.info(String.format("Starting the processing of %s", uri));            
            try {
                this.metadata.process(uri);
                save(this.metadata, uri);
                synchronized (this.attributes) {
                    int nbFiles = (Integer) this.attributes.get("nbFiles");
                    nbFiles++;
                    this.attributes.put("nbFiles", nbFiles);
                }
                progressPercentage(this.startTime, (boolean) this.attributes.get("isCounted"), (int) this.attributes.get("nbFiles"), (int) this.attributes.get("nbTotalFiles"));
                final long endProcessing = System.currentTimeMillis();
                final float timeProcessing = (float) ((endProcessing-startProcessing) / 1000.0f);
                LOGGER.info("processed file in "+timeProcessing+" s");
            } catch (Exception ex) {
                final long endProcessing = System.currentTimeMillis();
                final float timeProcessing = (float) ((endProcessing-startProcessing) / 1000.0f);                
                LOGGER.error("Unable to process the file - "+timeProcessing+" s", ex);
            }
        }
    }

    private Map<String, Object> getVariables(Metadata metadata, List<String> keywords) {
        Map<String, Object> variables = new HashMap<>();
        keywords.stream().filter((keyword) -> !(keyword.equals("lon") || keyword.equals("lat"))).forEach((keyword) -> {
            storeVariable(metadata.getData(keyword), variables, keyword);
        });
        return variables;
    }

    private void storeVariable(Object valueObj, Map<String, Object> variables, String keyword) {
        if (valueObj == null) {
            return;
        }
        if (valueObj instanceof Array) {
            Array val = (Array) valueObj;
            String datatype = val.getElementType().getCanonicalName();
            switch (datatype) {
                case "double":
                    double[] doubleValues = (double[]) val.copyTo1DJavaArray();
                    variables.put(keyword, doubleValues);
                    break;
                case "byte":
                    byte[] byteValues = (byte[]) val.copyTo1DJavaArray();
                    String[] values = new String[byteValues.length];
                    Map<Integer, String> mapping = metadata.getMapping(keyword);
                    for (int i = 0; i < byteValues.length; i++) {
                        Byte byteVal = byteValues[i];
                        int valInt = byteVal.intValue();
                        String desc;
                        if (mapping == null) {
                            desc = String.valueOf(valInt);
                        } else {
                            desc = mapping.get(valInt);
                        }
                        values[i] = desc;
                    }
                    variables.put(keyword, values);
                    break;
            }
        } else if (valueObj instanceof String[]) {
            String[] times = (String[]) valueObj;
            variables.put(keyword, times);
        }
    }

    private Map<String, Object> getServices(String uri) {
        Map<String, Object> download = new HashMap<>();
        download.put("mimetype", "application/x-netcdf");
        download.put("url", uri);
        Map<String, Object> services = new HashMap<>();
        services.put("download", download);
        return services;
    }

    private void save(Metadata metadata, String uri) throws URISyntaxException, FileNotFoundException, IOException {
        String fileName = uri.substring(uri.lastIndexOf('/') + 1, uri.length());
        String fileNameWithoutExtension = fileName.substring(0, fileName.lastIndexOf('.'));
        //String fileExtension = uri.substring(uri.lastIndexOf("."));        
        Feature feature = writer.createFeature();
        feature.setId(fileName);
        feature.setGeometry(createGeometry(metadata));
        feature.getProperties().putAll(metadata.getGlobalMetadata());
        feature.getProperties().put("variables", getVariables(metadata, KEYWORDS_TO_EXTRACT));
        feature.getForeignMembers().put("services", getServices(uri));
        FeatureCollection fc = writer.createFeatureCollection();
        fc.getFeatures().add(feature);       
        try (FileOutputStream fos = new FileOutputStream(new File(Settings.getInstance().getString(Consts.OUTPUT) + fileNameWithoutExtension + ".geojson"))) {
            String result = writer.toJson(fc);
            fos.write(result.getBytes());
        }
    }

    private Geometry createGeometry(Metadata extVariable) {
        LineString geom = writer.createGeometry(LineString.class);
        Array longitudeVariable = extVariable.getData("lon");
        Array latitudeVariable = extVariable.getData("lat");
        double[] longitudes = (double[]) longitudeVariable.copyTo1DJavaArray();
        double[] latitudes = (double[]) latitudeVariable.copyTo1DJavaArray();
        double[][] coordinates = new double[longitudes.length][];
        for (int i = 0; i < longitudes.length; i++) {
            coordinates[i] = new double[]{longitudes[i], latitudes[i]};
        }
        geom.setPoints(coordinates);
        return geom;
    }

    public static void progressPercentage(long startTime, boolean isFinishedToCount, int remain, int total) {
        if (isFinishedToCount) {
            int maxBareSize = 10; // 10unit for 100%
            int remainProcent = ((100 * remain) / total) / maxBareSize;
            char defaultChar = '-';
            String icon = "*";
            String bare = new String(new char[maxBareSize]).replace('\0', defaultChar) + "]";
            StringBuilder bareDone = new StringBuilder();
            bareDone.append("[");
            for (int i = 0; i < remainProcent; i++) {
                bareDone.append(icon);
            }
            String bareRemain = bare.substring(remainProcent, bare.length());
            long currentTime = System.currentTimeMillis();

            long diffTimePerFile = (currentTime - startTime) / remain;
            long estimatedTimeToFinish = (total - remain) * diffTimePerFile;
            float estimatedTimeToFinishInMn = estimatedTimeToFinish / 60000.0f;
            String time = String.format("%.2f", estimatedTimeToFinishInMn);
            System.out.print("\r" + bareDone + bareRemain + " " + remainProcent * 10 + "%" + "(" + remain + "/" + total + ")  - Still " + time + " mn");
            if (remain == total) {
                System.out.print("\n");
            }
        }
    }
}
