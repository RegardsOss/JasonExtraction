/*
 * Copyright 1997-2010 Unidata Program Center/University Corporation for
 * Atmospheric Research, P.O. Box 3000, Boulder, CO 80307,
 * support@unidata.ucar.edu.
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or (at
 * your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package fr.cnes.export.jason;

import ucar.ma2.Array;

/**
 *
 * @author Jean-Christophe Malapert <jean-christophe.malapert@cnes.fr>
 */
public class Utils {


    /**
     * Get the 1D values for an array as floats.
     *
     * @param arr Array of values
     * @return float representation
     */
    public static float[] toFloatArray(Array arr) {
        Class fromClass = arr.getElementType();
        if (fromClass.equals(float.class)) {
            // It should always be a float
            return (float[]) arr.get1DJavaArray(float.class);
        } else {
            float[] values = new float[(int) arr.getSize()];
            boolean isUnsigned = arr.isUnsigned();
            if (fromClass.equals(byte.class)) {
                byte[] fromArray = (byte[]) arr.get1DJavaArray(byte.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    if (isUnsigned) {
                        values[i] = (int) fromArray[i] & 0xFF;
                    } else {
                        values[i] = fromArray[i];
                    }
                }
            } else if (fromClass.equals(short.class)) {
                short[] fromArray = (short[]) arr.get1DJavaArray(short.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    if (isUnsigned) {
                        values[i] = (int) fromArray[i] & 0xFFFF;
                    } else {
                        values[i] = fromArray[i];
                    }
                }
            } else if (fromClass.equals(int.class)) {
                int[] fromArray = (int[]) arr.get1DJavaArray(int.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    if (isUnsigned) {
                        values[i] = (long) fromArray[i] & 0xFFFFFFFF;
                    } else {
                        values[i] = fromArray[i];
                    }
                }
            } else if (fromClass.equals(double.class)) {
                double[] fromArray = (double[]) arr.get1DJavaArray(double.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    values[i] = (float) fromArray[i];
                }
            } else {
                throw new IllegalArgumentException("Unknown array type:" + fromClass.getName());
            }
            return values;
        }

    }

    /**
     * Get the 1D values for an array as doubles.
     *
     * @param arr Array of values
     * @return double representation
     */
    public static double[] toDoubleArray(Array arr) {
        Class fromClass = arr.getElementType();
        if (fromClass.equals(double.class)) {
            // It should always be a double
            return (double[]) arr.get1DJavaArray(double.class);
        } else {
            double[] values = new double[(int) arr.getSize()];
            boolean isUnsigned = arr.isUnsigned();
            if (fromClass.equals(byte.class)) {
                byte[] fromArray = (byte[]) arr.get1DJavaArray(byte.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    if (isUnsigned) {
                        values[i] = (int) fromArray[i] & 0xFF;
                    } else {
                        values[i] = fromArray[i];
                    }
                }
            } else if (fromClass.equals(short.class)) {
                short[] fromArray = (short[]) arr.get1DJavaArray(short.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    if (isUnsigned) {
                        values[i] = (int) fromArray[i] & 0xFFFF;
                    } else {
                        values[i] = fromArray[i];
                    }
                }
            } else if (fromClass.equals(int.class)) {
                int[] fromArray = (int[]) arr.get1DJavaArray(int.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    if (isUnsigned) {
                        values[i] = (long) fromArray[i] & 0xFFFFFFFF;
                    } else {
                        values[i] = fromArray[i];
                    }
                }
            } else if (fromClass.equals(float.class)) {
                float[] fromArray = (float[]) arr.get1DJavaArray(float.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    values[i] = fromArray[i];
                }
            }
            return values;
        }

    }

    /**
     * Get the 1D values for an array as Strings.
     *
     * @param arr Array of values
     * @return String representation
     */
    public static String[] toStringArray(Array arr) {
        return (String[]) arr.get1DJavaArray(String.class);
    }

    /**
     * Get the 1D values for an array as chars.
     *
     * @param arr Array of values
     * @return chars representation
     */
    public static char[] toCharArray(Array arr) {
        Class fromClass = arr.getElementType();

        if (fromClass.equals(char.class)) {
            // It should always be a char
            return (char[]) arr.get1DJavaArray(char.class);
        } else {
            char[] values = new char[(int) arr.getSize()];
            boolean isUnsigned = arr.isUnsigned();
            if (fromClass.equals(byte.class)) {
                byte[] fromArray = (byte[]) arr.get1DJavaArray(byte.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    if (isUnsigned) {
                        values[i] = (char) ((int) fromArray[i] & 0xFF);
                    } else {
                        values[i] = (char) fromArray[i];
                    }
                }
            } else if (fromClass.equals(short.class)) {
                short[] fromArray = (short[]) arr.get1DJavaArray(short.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    values[i] = (char) fromArray[i];
                }
            } else if (fromClass.equals(int.class)) {
                int[] fromArray = (int[]) arr.get1DJavaArray(int.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    values[i] = (char) fromArray[i];
                }
            } else if (fromClass.equals(float.class)) {
                float[] fromArray = (float[]) arr.get1DJavaArray(float.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    values[i] = (char) fromArray[i];
                }
            } else if (fromClass.equals(double.class)) {
                double[] fromArray
                        = (double[]) arr.get1DJavaArray(double.class);
                for (int i = 0; i < fromArray.length; ++i) {
                    values[i] = (char) fromArray[i];
                }
            }
            return values;
        }

    }

}
