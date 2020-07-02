package edu.emory.pathology.export4gd;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author Geofrey H. Smith
 */
@XmlRootElement
public class CoPathCase {
    
    @XmlRootElement
    public static class CoPathProcedure {
    
        @XmlAttribute
        public String procName;
        public String interp;
        public String comment;
        public String clinHist;
        public String procPathLastName;
        public String procPathFirstName;
        public String soPathLastName;
        public String soPathFirstName;

        public CoPathProcedure() {
        }

        public CoPathProcedure(ResultSet rs) throws SQLException {
            this.procName = rs.getString("proc_name");
            this.interp = rs.getString("procint_text") == null ? null : rs.getString("procint_text")
                .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
                .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
                .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
                .replace("\r", "")
                .replaceAll("\\s+$", "");
            this.comment = rs.getString("procres_text") == null ? null : rs.getString("procres_text")
                .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
                .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
                .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
                .replace("\r", "")
                .replaceAll("\\s+$", "");
            this.clinHist = rs.getString("procclinhist_text") == null ? null : rs.getString("procclinhist_text")
                .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
                .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
                .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
                .replace("\r", "")
                .replaceAll("\\s+$", "");
            this.procPathLastName = rs.getString("procpath_lastname");
            this.procPathFirstName = rs.getString("procpath_firstname");
            this.soPathLastName = rs.getString("sopath_lastname");
            this.soPathFirstName = rs.getString("sopath_firstname");
        }
        
    }

    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy"); 
    
    @XmlTransient
    public String specimenId;
    @XmlAttribute
    public String accNo;
    @XmlTransient
    public Date accessionDate;
    @XmlTransient
    public Date collectionDate;
    @XmlAttribute
    public String empi;
    public String finalDiagnosis;
    public String karyotype;
    @XmlElementWrapper(name = "procedures")
    @XmlElement(name = "procedure")
    public List<CoPathProcedure> procedures;
    @XmlTransient
    private Map<String, CoPathProcedure> procedureMap;

    public CoPathCase() {
    }

    public CoPathCase(ResultSet rs) throws SQLException {
        this.specimenId = rs.getString("specimen_id");
        this.accNo = rs.getString("specnum_formatted");
        this.accessionDate = rs.getDate("accession_date");
        this.collectionDate = rs.getDate("datetime_taken");
        this.empi = rs.getString("universal_mednum_stripped");
        this.finalDiagnosis = rs.getString("final_text") == null ? null : rs.getString("final_text")
            .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
            .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
            .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
            .replace("\r", "")
            .replaceAll("\\s+$", "");
        this.karyotype = rs.getString("karyotype") == null ? null : rs.getString("karyotype")
            .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
            .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
            .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
            .replace("\r", "")
            .replaceAll("\\s+$", "");
    }

    @XmlAttribute
    public String getAccessionDate() {
        return sdf.format(this.accessionDate);
    }

    public void setAccessionDate(String accessionDate) throws ParseException {
        this.accessionDate = new Date(sdf.parse(accessionDate).getTime());
    }

    @XmlAttribute
    public String getCollectionDate() {
        return sdf.format(this.collectionDate);
    }

    public void setCollectionDate(String collectionDate) throws ParseException {
        this.collectionDate = new Date(sdf.parse(collectionDate).getTime());
    }

    @XmlTransient
    public Map<String, CoPathProcedure> getProcedureMap() {
        if(procedureMap == null) {
            procedureMap = new HashMap<>();
            for(CoPathProcedure coPathProcedure : procedures) {
                procedureMap.put(coPathProcedure.procName, coPathProcedure);
            }
        }
        return procedureMap;
    }
    
}
