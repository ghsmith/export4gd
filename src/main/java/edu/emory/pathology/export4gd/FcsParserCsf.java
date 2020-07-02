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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author ghsmith
 */
public class FcsParserCsf {

    private static final Logger LOGGER = Logger.getLogger(FcsParserCsf.class.getName());
    
    static SimpleDateFormat sdfIn = new SimpleDateFormat("dd-MMM-yyyy");
    static SimpleDateFormat sdfOut = new SimpleDateFormat("MM/dd/yyyy"); 
    
    public static class Record {
        // hidden
        public String fcsFileDirectory;
        public String fcsFileCaseName;
        public String fcsFileTubeNo;
        public String fcsFileSeqNo;
        public String fcsCaseNumber;
        public String fcsSampleId;
        public String[][] parameterNames = new String[20][2];
        // exposed
        public String cpCaseEmpi;
        public String cpCaseCollDate;
        public String fcsDate;
        public String cpCaseAccNo;
        public String cpFlowProcClinHist;
        public String cpFlowProcInterp;
        public String precluded;
        public String fcsTubeName;
        public String cpFlowProcPathBilling;
        public String cpFlowProcPathSignOut;
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(cpCaseEmpi + ",");
            sb.append(cpCaseCollDate + ",");
            sb.append(fcsDate + ",");
            sb.append(cpCaseAccNo + ",");
            sb.append("\"" + cpFlowProcClinHist + "\",");
            sb.append("\"" + cpFlowProcInterp + "\",");
            sb.append(precluded + ",");
            sb.append(fcsTubeName + ",");
            sb.append(cpFlowProcPathBilling + ",");
            sb.append(cpFlowProcPathSignOut + ",");
            return sb.toString().substring(0, sb.toString().length() - 1);
        }
    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, ParseException {

        Properties priv = new Properties();
        try(InputStream inputStream = FcsParserCsf.class.getClassLoader().getResourceAsStream("private.properties")) {
            priv.load(inputStream);
        }
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection connCoPath = DriverManager.getConnection(priv.getProperty("connCoPath.url"));
        
        CoPathCaseFinder cpcf = new CoPathCaseFinder(connCoPath);
        
        Record recordHeader = new Record();
        recordHeader.cpCaseEmpi = "cpCaseEmpi";
        recordHeader.cpCaseCollDate = "cpCaseCollDate";
        recordHeader.fcsDate = "fcsDate";
        recordHeader.cpCaseAccNo = "cpCaseAccNo";
        recordHeader.cpFlowProcClinHist = "cpFlowProcClinHist";
        recordHeader.cpFlowProcInterp = "cpFlowProcInterp";
        recordHeader.precluded = "precluded";
        recordHeader.fcsTubeName = "fcsTubeName";
        recordHeader.cpFlowProcPathBilling = "cpFlowProcPathBilling";
        recordHeader.cpFlowProcPathBilling = "cpFlowProcPathBilling";
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
                record.fcsFileDirectory = matcherFcsFileName1.group(1);
                record.fcsFileCaseName = matcherFcsFileName1.group(2);
                record.fcsFileTubeNo = matcherFcsFileName1.group(3);
                record.fcsFileSeqNo = matcherFcsFileName1.group(4);
            }
            else if(matcherFcsFileName2.find()) {
                record.fcsFileDirectory = matcherFcsFileName2.group(1);
                record.fcsFileCaseName = matcherFcsFileName2.group(2);
                record.fcsFileTubeNo = matcherFcsFileName2.group(3);
            }
            else if(matcherFcsFileName3.find()) {
                record.fcsFileDirectory = matcherFcsFileName3.group(1);
                record.fcsFileCaseName = matcherFcsFileName3.group(2);
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
            record.fcsDate = sdfOut.format(sdfIn.parse(dataMap.get("$DATE")));
            record.fcsCaseNumber = dataMap.get("CASE NUMBER");
            record.fcsTubeName = dataMap.get("TUBE NAME");
            record.fcsSampleId = dataMap.get("SAMPLE ID");
            for(int x = 0; x < 20; x++) {
                record.parameterNames[x][0] = dataMap.get("$P" + (x + 1) + "N");
                record.parameterNames[x][1] = dataMap.get("$P" + (x + 1) + "S");
                if(record.parameterNames[x][1] != null) {
                    record.parameterNames[x][1] = record.parameterNames[x][1].replace(record.parameterNames[x][0], "");
                    record.parameterNames[x][1] = record.parameterNames[x][1].trim();
                }
            }

            LOGGER.info("sample ID is " + record.fcsSampleId);
            if(!"CSF".equals(record.fcsSampleId)) {
                continue;
            }
            
            if(record.fcsCaseNumber != null) {
                LOGGER.info("looking for CoPath " + record.fcsCaseNumber.trim().replaceAll(" {2,}", " ").replaceAll(" ", "-"));
                CoPathCase cpc = cpcf.getCoPathCaseByAccNo(record.fcsCaseNumber.trim().replaceAll(" {2,}", " ").replaceAll(" ", "-"));
                if(cpc != null) {
                    record.cpCaseEmpi = cpc.empi;
                    record.cpCaseAccNo = cpc.accNo;
                    record.cpCaseCollDate = cpc.getCollectionDate();
                    record.cpFlowProcClinHist = cpc.getProcedureMap().get("Flow Cytometry") == null || cpc.getProcedureMap().get("Flow Cytometry").clinHist == null ? "" : cpc.getProcedureMap().get("Flow Cytometry").clinHist.replace("\"", "'");
                    record.cpFlowProcInterp = cpc.getProcedureMap().get("Flow Cytometry") == null || cpc.getProcedureMap().get("Flow Cytometry").interp == null ? "" : cpc.getProcedureMap().get("Flow Cytometry").interp.replace("\"", "'");
                    record.cpFlowProcPathBilling = cpc.getProcedureMap().get("Flow Cytometry") == null || cpc.getProcedureMap().get("Flow Cytometry").procPathLastName == null ? "" : cpc.getProcedureMap().get("Flow Cytometry").procPathLastName;
                    record.cpFlowProcPathSignOut = cpc.getProcedureMap().get("Flow Cytometry") == null || cpc.getProcedureMap().get("Flow Cytometry").soPathLastName == null ? "" : cpc.getProcedureMap().get("Flow Cytometry").soPathLastName;
                }
            }
            
            System.out.println(record);    

        }

    }
    
}
