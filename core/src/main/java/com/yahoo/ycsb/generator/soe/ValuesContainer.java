package com.yahoo.ycsb.generator.soe;

import java.util.HashMap;
import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

/**
 * Created by oleksandr.gyryk on 8/31/17.
 */
public final class ValuesContainer {

  public static final String DEFAULTS_CUSTOMERDOCNAME_PREFIX = "customer:::";
  public static final String DOCUMENT = "soedocument";
  public static final String OBJ_ADDRES_FLD_ZIP = "address.zip";
  public static final String OBJ_ADDRES_FLD_COUNTRY = "address.country";
  public static final String FLD_AGEGROUP = "age_group";
  public static final String FLD_DOBYEAR = "dob";
  public static final String OBJ_ADDRESS_OBJ_PREVADDRES_FLD_ZIP = "address.prev_address.zip";
  public static final String FLD_DEVICES = "devices";
  public static final String OBJ_VISITEDPLACES_FLD_COUNTRY = "visited_places.country";
  public static final String OBJ_VISITEDPLACES_FLD_CITIES = "visited_places.cities";
  public static final String FLD_ORDERMONTH = "month";
  public static final String FLD_ORDERSALEPRICE = "sale_price";
  public static final String FLD_ORDERLIST = "order_list";

  public static HashMap<String, String[]> getData() {
    return data;
  }
  private static HashMap<String, String[]> data = new HashMap<>();

  public static void init(String path) {
    data.put(DOCUMENT, importDocuments(path));
    data.put(OBJ_ADDRES_FLD_ZIP, importZips());
    data.put(OBJ_ADDRES_FLD_COUNTRY, importContries());
    data.put(FLD_AGEGROUP, importAgeGroups());
    data.put(FLD_DOBYEAR, importDBYears());
    data.put(OBJ_ADDRESS_OBJ_PREVADDRES_FLD_ZIP, importZips());
    data.put(FLD_DEVICES, importDevices());
    data.put(OBJ_VISITEDPLACES_FLD_COUNTRY, importContries());
    data.put(OBJ_VISITEDPLACES_FLD_CITIES, importCities());
    data.put(FLD_ORDERMONTH, importOrderMonth());
  }

  private ValuesContainer() {};

  private static String[] importDocuments(String path) {
    ArrayList<String> jsonStrings = new ArrayList<>();

    File[] files = new File(path).listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith(".json");
      }
    });

    try {
      for (File file : files) {
        byte[] encoded = Files.readAllBytes(Paths.get(path + "/" + file.getName()));
        jsonStrings.add(new String(encoded, StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      System.err.println("Error reading document  in " + path);
      System.exit(1);
    }

    return jsonStrings.toArray(new String[jsonStrings.size()]);
  }

  private static  String[] importZips() {
    return new String[] {"90001", "90002",  "90003", "90004",  "90005", "90006",  "90007", "90008",  "90009", "90010"};
  }

  private static  String[] importContries() {
    return new String[]{"USA", "Ukraine", "Canada", "Spain", "Germany"};
  }

  private static  String[] importAgeGroups() {
    return new String[]{"child", "teen", "adult", "senior"};
  }

  private static  String[] importDBYears() {
    return new String[]{"2016", "2017"};
  }

  private static  String[] importDevices() {
    return new String[]{"AA-1", "BB-9", "CC-0", "FF-6"};
  }

  private static  String[] importCities() {
    return new String[]{"NY", "San Francisco", "Chicago", "Los Angeles"};
  }

  private static  String[] importOrderMonth() {
    return new String[]{"January", "February", "March", "April"};
  }

}

