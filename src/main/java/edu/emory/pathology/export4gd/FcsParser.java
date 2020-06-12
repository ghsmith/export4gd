package edu.emory.pathology.export4gd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author ghsmith
 */
public class FcsParser {

    private static final Logger LOGGER = Logger.getLogger(FcsParser.class.getName());
    
    static SimpleDateFormat sdfIn = new SimpleDateFormat("dd-MMM-yyyy");
    static SimpleDateFormat sdfOut = new SimpleDateFormat("MM/dd/yyyy"); 
    
    public static class Record {
        public String date;
        public String expirementName;
        public String tubeName;
        public String sample;
        public String caseNumber;
        public String directory;
        public String caseName;
        public String tubeNo;
        public String seqNo;
        public String[][] parameterNames = new String[20][2];
        public String coPathAccNo;
        public String coPathCollectionDate;
        public String coPathAccessionDate;
        public String coPathFlowInterp;
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(date + ",");
            sb.append(expirementName + ",");
            sb.append(tubeName + ",");
            sb.append(sample + ",");
            sb.append(caseNumber + ",");
            sb.append(directory + ",");
            sb.append(caseName + ",");
            sb.append(tubeNo + ",");
            sb.append(seqNo + ",");
            for(int x = 0; x < parameterNames.length; x++) {
                sb.append((parameterNames[x][0] != null ? parameterNames[x][0] : "") + ",");
                sb.append((parameterNames[x][1] != null ? parameterNames[x][1] : "") + ",");
            }
            sb.append(coPathAccNo + ",");
            sb.append(coPathCollectionDate + ",");
            sb.append(coPathAccessionDate + ",");
            sb.append("\"" + coPathFlowInterp + "\",");
            return sb.toString().substring(0, sb.toString().length() - 1);
        }
    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, ParseException {

        Properties priv = new Properties();
        try(InputStream inputStream = FcsParser.class.getClassLoader().getResourceAsStream("private.properties")) {
            priv.load(inputStream);
        }
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection connCoPath = DriverManager.getConnection(priv.getProperty("connCoPath.url"));
        
        CoPathCaseFinder cpcf = new CoPathCaseFinder(connCoPath);
        
        //List<Record> recordList = new ArrayList<>();
        Record recordHeader = new Record();
        recordHeader.date = "date";
        recordHeader.expirementName = "expirementName";
        recordHeader.tubeName = "tubeName";
        recordHeader.sample = "sample";
        recordHeader.caseNumber = "caseNumber";
        recordHeader.directory = "file_directory";
        recordHeader.caseName = "file_caseName";
        recordHeader.tubeNo = "file_tubeNo";
        recordHeader.seqNo = "file_seqNo";
        for(int x = 0; x < 20; x++) {
            recordHeader.parameterNames[x][0] = "p" + (x + 1) + "Anal";
            recordHeader.parameterNames[x][1] = "p" + (x + 1) + "Name";
        }
        recordHeader.coPathAccNo = "coPath_accNo";
        recordHeader.coPathCollectionDate = "coPath_collDt";
        recordHeader.coPathAccessionDate = "coPath_accDt";
        recordHeader.coPathFlowInterp = "coPath_flowInterp";
        //recordList.add(recordHeader);
        System.out.println(recordHeader);
        
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));        
        String fcsFileName;
        Pattern patternFcsFileName1 = Pattern.compile("^./.*/(.*)/(.*)_(.*)_(.*)\\.fcs$");
        Pattern patternFcsFileName2 = Pattern.compile("^./.*/(.*)/(.*)_(.*)\\.fcs$");
        Pattern patternFcsFileName3 = Pattern.compile("^./.*/(.*)/(.*)\\.fcs$");
        while((fcsFileName = stdIn.readLine()) != null && fcsFileName.length() != 0) {
            LOGGER.info("reading file " + fcsFileName);
            Matcher matcherFcsFileName1 = patternFcsFileName1.matcher(fcsFileName);
            Matcher matcherFcsFileName2 = patternFcsFileName2.matcher(fcsFileName);
            Matcher matcherFcsFileName3 = patternFcsFileName3.matcher(fcsFileName);
            Record record = new Record();
            if(matcherFcsFileName1.find()) {
                record.directory = matcherFcsFileName1.group(1);
                record.caseName = matcherFcsFileName1.group(2);
                record.tubeNo = matcherFcsFileName1.group(3);
                record.seqNo = matcherFcsFileName1.group(4);
            }
            else if(matcherFcsFileName2.find()) {
                record.directory = matcherFcsFileName2.group(1);
                record.caseName = matcherFcsFileName2.group(2);
                record.tubeNo = matcherFcsFileName2.group(3);
            }
            else if(matcherFcsFileName3.find()) {
                record.directory = matcherFcsFileName3.group(1);
                record.caseName = matcherFcsFileName3.group(2);
            }
            else {
                LOGGER.severe("unparseable file name");
                continue;
            }
            byte[] fcsFileBytes = Files.readAllBytes(Paths.get(fcsFileName));
            int dataStart = Integer.parseInt(new String(Arrays.copyOfRange(fcsFileBytes, 10, 18)).trim());
            int dataEnd = Integer.parseInt(new String(Arrays.copyOfRange(fcsFileBytes, 18, 26)).trim());
            String delimiter = new String(new String(Arrays.copyOfRange(fcsFileBytes, dataStart, dataStart + 1)));
            String[] data = new String(new String(Arrays.copyOfRange(fcsFileBytes, dataStart + 1, dataEnd + 1))).split(delimiter);
            Map<String, String> dataMap = new HashMap<>();
            for(int x = 0; x < data.length; x += 2) {
                dataMap.put(data[x], data[x + 1]);
            }
            record.date = sdfOut.format(sdfIn.parse(dataMap.get("$DATE")));
            record.expirementName = dataMap.get("EXPERIMENT NAME");
            record.tubeName = dataMap.get("TUBE NAME");
            record.sample = dataMap.get("SAMPLE ID");
            record.caseNumber = dataMap.get("CASE NUMBER");
            for(int x = 0; x < 20; x++) {
                record.parameterNames[x][0] = dataMap.get("$P" + (x + 1) + "N");
                record.parameterNames[x][1] = dataMap.get("$P" + (x + 1) + "S");
                if(record.parameterNames[x][1] != null) {
                    record.parameterNames[x][1] = record.parameterNames[x][1].replace(record.parameterNames[x][0], "");
                    record.parameterNames[x][1] = record.parameterNames[x][1].trim();
                }
            }

            if(record.caseNumber != null) {
                LOGGER.info("looking for CoPath " + record.caseNumber.trim().replaceAll(" {2,}", " ").replaceAll(" ", "-"));
                CoPathCase cpc = cpcf.getCoPathCaseByAccNo(record.caseNumber.trim().replaceAll(" {2,}", " ").replaceAll(" ", "-"));
                if(cpc != null) {
                    record.coPathAccNo = cpc.accNo;
                    record.coPathCollectionDate = cpc.getCollectionDate();
                    record.coPathAccessionDate = cpc.getAccessionDate();
                    record.coPathFlowInterp = cpc.getProcedureMap().get("Flow Cytometry") == null || cpc.getProcedureMap().get("Flow Cytometry").interp == null ? "" : cpc.getProcedureMap().get("Flow Cytometry").interp.replace("\"", "'");
                }
            }
            
            //recordList.add(record);
            System.out.println(record);    
        }

        //for(Record record : recordList) {
        //    System.out.println(record);
        //}
        
    }
    
}
