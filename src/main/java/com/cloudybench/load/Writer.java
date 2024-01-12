package com.cloudybench.load;
/**
 *
 * @time 2023-03-04
 * @version 1.0.0
 * @file Writer.java
 * @description
 *  write data into cvs
 **/
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class Writer {
    public void write2csv(List<Record> lst, String FileName){
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter=null;
        String DataPath="Data/";
        String NEW_LINE_SEPARATOR = "\n";
        try {
            File directory=new File(DataPath);
            if(!directory.exists())
                directory.mkdirs();
            fileWriter = new FileWriter(DataPath+FileName,false);
            bufferedWriter = new BufferedWriter(fileWriter);
            for (int i=0;i<lst.size();i++) {
                bufferedWriter.write(lst.get(i).toString());
                bufferedWriter.write(NEW_LINE_SEPARATOR);
            }
        } catch (Exception e) {
            System.out.println("Error in Writer!");
            e.printStackTrace();
        } finally {
            try {
                bufferedWriter.flush();
                bufferedWriter.close();
            } catch (IOException e) {
                System.out.println("Error while flushing/closing fileWriter !");
                e.printStackTrace();
            }
        }
    }
}