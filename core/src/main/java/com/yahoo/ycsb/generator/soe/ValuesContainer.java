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
  public static final String OBJ_ADDRES_FLD_COUNTRY = "address.country";
  public static final String FLD_AGEGROUP = "age_group";
  public static final String FLD_DOBYEAR = "dob";
  public static final String OBJ_ADDRESS_OBJ_PREVADDRES_FLD_COUNTRY = "address.prev_address.country";
  public static final String FLD_DEVICES = "devices";
  public static final String OBJ_VISITEDPLACES_FLD_COUNTRY = "visited_places.country";
  public static final String OBJ_VISITEDPLACES_FLD_ACTIVITIES = "visited_places.activities";
  public static final String FLD_ORDERMONTH = "month";
  public static final String FLD_ORDERSALEPRICE = "sale_price";
  public static final String FLD_ORDERLIST = "order_list";

  public static final String ALLCUSTOMERFIELDS = "allfields";


  public static HashMap<String, String[]> getData() {
    return data;
  }
  private static HashMap<String, String[]> data = new HashMap<>();

  public static void init(String path) {
    data.put(DOCUMENT, importDocuments(path));
    data.put(OBJ_ADDRES_FLD_COUNTRY, importCountries());
    data.put(FLD_AGEGROUP, importAgeGroups());
    data.put(FLD_DOBYEAR, importDBYears());
    data.put(OBJ_ADDRESS_OBJ_PREVADDRES_FLD_COUNTRY, importCountries());
    data.put(FLD_DEVICES, importDevices());
    data.put(OBJ_VISITEDPLACES_FLD_COUNTRY, importCountries());
    data.put(OBJ_VISITEDPLACES_FLD_ACTIVITIES, importActivities());
    data.put(FLD_ORDERMONTH, importOrderMonth());
    data.put(ALLCUSTOMERFIELDS, listAllCustomerFields());
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

  private static  String[] importCountries() {
    return new String[]{"Belgium", "France", "Germany", "Italy", "Luxembourg", "Netherlands", "Denmark", "Ireland",
                        "United Kingdom", "Greece", "Portugal", "Spain", "Austria", "Finland", "Sweden", "Cyprus",
                        "Czech Republic", "United States", "Canada", "Estonia", "Hungary",  "Latvia", "Lithuania",
                        "Malta", "Poland",  "Slovakia", "Slovenia", "Bulgaria", "Romania", "Croatia"};
  }


  private static  String[] importAgeGroups() {
    return new String[]{"child", "teen", "adult", "senior"};
  }

  private static  String[] importDBYears() {
    return new String[]{"1980", "1981", "1982", "1983", "1984", "1985", "1986", "1987", "1988", "1989",
                        "1990", "1991", "1992", "1993", "1994", "1994", "1996", "1997", "1998", "1999"};
  }

  private static  String[] importDevices() {
    return new String[]{"AA-0", "AA-1", "AA-2", "AA-3", "AA-4", "AA-5", "AA-6", "AA-7", "AA-8", "AA-9",
                        "AB-0", "AB-1", "AB-2", "AB-3", "AB-4", "AB-5", "AB-6", "AB-7", "AB-8", "AB-9",
                        "AC-0", "AC-1", "AC-2", "AC-3", "AC-4", "AC-5", "AC-6", "AC-7", "AC-8", "AC-9",
                        "BA-0", "BA-1", "BA-2", "BA-3", "BA-4", "BA-5", "BA-6", "BA-7", "BA-8", "BA-9",
                        "BB-0", "BB-1", "BB-2", "BB-3", "BB-4", "BB-5", "BB-6", "BB-7", "BB-8", "BB-9",
                        "BC-0", "BC-1", "BC-2", "BC-3", "BC-4", "BC-5", "BC-6", "BC-7", "BC-8", "BC-9",
                        "CA-0", "CA-1", "CA-2", "CA-3", "CA-4", "CA-5", "CA-6", "CA-7", "CA-8", "CA-9",
                        "CB-0", "CB-1", "CB-2", "CB-3", "CB-4", "CB-5", "CB-6", "CB-7", "CB-8", "CB-9",
                        "CC-0", "CC-1", "CC-2", "CC-3", "CC-4", "CC-5", "CC-6", "CC-7", "CC-8", "CC-9"};
  }

  private static  String[] importActivities() {
    return new String[]{"museum", "architecture", "shopping", "food", "show", "exibition", "theatre",
                        "education", "business", "dating", "nightlife", "water park", "amusement park",
                        "scenic", "camping", "sport", "health", "fashion", "family", "other"};
  }

  private static  String[] importOrderMonth() {
    return new String[]{"January", "February", "March", "April", "May", "June",
                        "July", "August", "September", "October", "November", "December"};
  }

  private static String[] listAllCustomerFields() {
    return new String[]{"_id", "doc_id", "gid", "first_name", "middle_name", "last_name", "ballance_current", "dob",
                        "email", "isActive", "linear_score", "weighted_score", "phone_country", "phone_by_country",
                        "age_group", "age_by_group", "url_protocol", "url_site", "url_domain", "url", "devices",
                        "linked_devices", "address", "children", "visited_places", "order_list"};
  }

}

