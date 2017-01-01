/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch;


import java.io.*;
import java.util.Properties;


public class Configurator {

    private static Configurator INSTANCE = null;
    private boolean heatmap;
    private boolean graph;
    private boolean uh;

    private File file;

    public Configurator(){
        configValue();
    }

    public static Configurator getInstance(){
        if (INSTANCE == null) {
            INSTANCE = new Configurator();
            INSTANCE.configValue();
        }
        else INSTANCE.configValue();
        return INSTANCE;
    }


    public void configValue(){
    Properties prop = new Properties();
                try {
                    file = new File("config.properties");
                    InputStream input =new FileInputStream(file);
                    prop.load(input);
                    heatmap = Boolean.valueOf(prop.getProperty("heatmap"));
                    graph = Boolean.valueOf(prop.getProperty("graph"));
                    uh = Boolean.valueOf(prop.getProperty("uh"));


                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
    }

    public boolean getHeatMapValue(){
        if(heatmap==false) {
            System.out.println("ITS FALSE OMG OMG ");
        }
        if(heatmap==true) {
            System.out.println("OMG ITS TRUE YAAAY");

        }
        return heatmap;}
    public boolean getGraphValue(){
        return graph;}
    public boolean getUhValue(){
        return uh;}



}
