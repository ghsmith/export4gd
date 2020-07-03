package edu.emory.pathology.export4gd;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author Geoffrey H. Smith
 */
@XmlRootElement
public class PathNetResult {

    @XmlAttribute
    public String accNo;
    @XmlAttribute
    public String resultName;
    @XmlAttribute
    public Integer collectionDateDelta;
    @XmlAttribute
    public String value;
    @XmlAttribute
    public String uom;
    @XmlAttribute
    public String flag;
    public String interp;

    public PathNetResult() {
    }

    public PathNetResult(ResultSet rs) throws SQLException {
        this.accNo = rs.getString("accno");
        this.resultName = rs.getString("result_name");
        this.collectionDateDelta = rs.getInt("collection_days_delta"); if(rs.wasNull()) { this.collectionDateDelta = null; }
        this.value = rs.getString("result_value");
        this.uom = rs.getString("result_uom");
        this.flag = rs.getString("result_flag");
        if(this.resultName.matches(".*(?i:interp).*") || this.resultName.matches("Urine Protein Electrophoresis")) {
            this.interp = rs.getString("result_narrative") == null ? null : rs.getString("result_narrative")
                .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
                .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
                .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
                .replace("\r", "")
                .replaceAll("(?m)\\s+$", "")
                .replaceAll("(?m)^", "")
                .replaceAll("^\n", "");
        }
        // for old PathNet Classic accession numbers...
        if(("SPEINTERP".equals(this.resultName) || "Urine Protein Electrophoresis".equals(this.resultName)) && (this.accNo == null || this.accNo.length() == 0) && this.interp != null) {
            Pattern pattern = Pattern.compile("Result:\\n\\t([0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9][0-9]*).*", Pattern.MULTILINE);
            Matcher matcher = pattern.matcher(this.interp);
            if(matcher.find()) {
                this.accNo = matcher.group(1).replace("-", "");
            }
        }
    }
    
}
