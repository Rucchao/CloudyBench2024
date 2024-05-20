package com.cloudybench.load;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class DataSource {
    static final ClassLoader loader = DataSource.class.getClassLoader();
    private static final String FirstName_female = "FirstName_Female";
    private static final String FirstName_male = "FirstName_Male";
    private static final String LastName = "LastName";
    private static final String Province2City = "CityByProvince";

    public List<String> FirstName_female_list = new ArrayList<String>();
    public List<String> FirstName_male_list = new ArrayList<String>();
    public List<String> LastName_list = new ArrayList<String>();
    public List<String> Province2City_list = new ArrayList<String>();
    public Set<String> ProvinceSet = new HashSet<>();
    public HashMap<String, List<String>> Province_Cities_Map = new HashMap<>();
    public  String[] gender  = {"female", "male"};

    public DataSource(){
        load(FirstName_female, FirstName_female_list);
        load(FirstName_male, FirstName_male_list);
        load(LastName, LastName_list);
        loadLocation(Province2City);
    }

    private void load(String fileName, List<String> lst){
        try {
            BufferedReader Br = new BufferedReader(
                    new InputStreamReader(loader.getResourceAsStream(fileName)));
            String line;
            while ((line = Br.readLine()) != null) {
                lst.add(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadLocation(String fileName){
        try {
            BufferedReader Br = new BufferedReader(
                    new InputStreamReader(loader.getResourceAsStream(fileName)));
            String line;
            while ((line = Br.readLine()) != null) {
                String Province = line.split("\\s+")[0].toString();
                String City = line.split("\\s+")[1].toString();
                if(!ProvinceSet.contains(Province)){
                    ProvinceSet.add(Province);
                    List<String> city_list = new ArrayList<String>();
                    city_list.add(City);
                    Province_Cities_Map.put(Province,city_list);
                }
                else {
                    List<String> city_list = Province_Cities_Map.get(Province);
                    city_list.add(City);
                    Province_Cities_Map.put(Province,city_list);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
