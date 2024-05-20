package com.cloudybench;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConfigLoader {
    Logger logger = LogManager.getLogger(ConfigLoader.class);
    public static String confFile = null;
    public static Properties prop = new Properties();

    public void loadConfig(){
        try {
            FileInputStream fis = new FileInputStream(ConfigLoader.confFile);
            prop.load(fis);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            logger.error("Read configure failed : " + ConfigLoader.confFile,e  );
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("Read configure failed : " + ConfigLoader.confFile,e);
            e.printStackTrace();
        }
    }

    public void printConfig(){
        logger.info("===============configuration==================");
        for(Object k:prop.keySet()){
            logger.info(k.toString() + " = " + prop.getProperty(k.toString()));
        }
        logger.info("===============configuration==================");
        logger.info("");
    }
}
