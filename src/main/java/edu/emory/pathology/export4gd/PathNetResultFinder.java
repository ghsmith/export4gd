package edu.emory.pathology.export4gd;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Geoffrey H. Smith
 */
public class PathNetResultFinder {
    
    private final Connection conn;
    private final PreparedStatement pstmt1;

    public PathNetResultFinder(Connection conn) throws SQLException {
        this.conn = conn;
        pstmt1 = conn.prepareStatement(
            "with proximate_lab_result_keys as                                                                                                                                      "
          + "(                                                                                                                                                                      "
          + "    select                                                                                                                                                             "
          + "      lsrt.structured_result_type_key,                                                                                                                                 "
          + "      max (frl.result_lab_key) keep                                                                                                                                    "
          + "      (                                                                                                                                                                "
          + "        dense_rank first order by                                                                                                                                      "
          + "          decode                                                                                                                                                       "
          + "            (                                                                                                                                                          "
          + "              sign(trunc(frl.specimen_collect_dt) - ?),                                                                                                                "
          + "               0, 0,                                                                                                                                                   "
          + "              -1, 1,                                                                                                                                                   "
          + "              +1, 2                                                                                                                                                    "
          + "            ) asc,                                                                                                                                                     "
          + "          abs(trunc(frl.specimen_collect_dt) - ?) asc                                                                                                                  "
          + "      ) result_lab_key                                                                                                                                                 "
          + "    from                                                                                                                                                               "
          + "      ehcvw.fact_result_lab frl                                                                                                                                        "
          + "      join                                                                                                                                                             "
          + "      (                                                                                                                                                                "
          + "        select                                                                                                                                                         "
          + "          structured_result_type_key                                                                                                                                   "
          + "        from                                                                                                                                                           "
          + "          ehcvw.lkp_structured_result_type                                                                                                                             "
          + "        where                                                                                                                                                          "
          + "          structured_result_type_key in                                                                                                                                 "
          + "          (                                                                                                                                                            "
+" 16504"
+",16509"
+",34558"
+",16511"
+",16512"
+",16525"
+",16529"
+",34559"
+",16519"
+",16528"
+",16531"
+",16532"
+",34560"
+",16539"
+",16540"
+",16546"
+",16549"
+",34561"
+",16556"
+",16551"
+",16515"
+",16517"
+",16523"
+",16530"
+",16541"
+",16544"
+",16561"
+",16554"
          + "          )                                                                                                                                                            "
          + "      ) lsrt on (lsrt.structured_result_type_key = frl.structured_result_type_key)                                                                                     "
          + "    where                                                                                                                                                              "
          + "      frl.patient_key in (select patient_key from ehcvw.lkp_patient where empi_nbr = ?)                                                                                "
          + "      and frl.specimen_collect_dt >= ? - 0                                                                                                                             "
          + "      and frl.specimen_collect_dt <= ? + 1                                                                                                                             "
          + "    group by                                                                                                                                                           "
          + "      lsrt.structured_result_type_key                                                                                                                                  "
          + ")                                                                                                                                                                      "
          + "select                                                                                                                                                                 "
          + "  frl.accession_nbr accno,                                                                                                                                             "
          + "  lsrt.structured_result_type_desc result_name,                                                                                                                        "
          + "  frl.result_lab_tval result_value,                                                                                                                                    "
          + "  lum.unit_measure_desc result_uom,                                                                                                                                    "
          + "  lri.result_interpretation_desc result_flag,                                                                                                                          "
          + "  trunc(frl.specimen_collect_dt) - ? collection_days_delta,                                                                                                            "
          + "  (select listagg(event_document_abstract_txt, ';') within group (order by order_key) from ehcvw.fact_event_document where order_key = frl.order_key) result_narrative "
          + "from                                                                                                                                                                   "
          + "  (                                                                                                                                                                    "
          + "    select                                                                                                                                                             "
          + "      structured_result_type_key,                                                                                                                                      "
          + "      structured_result_type_desc                                                                                                                                      "
          + "    from                                                                                                                                                               "
          + "      ehcvw.lkp_structured_result_type                                                                                                                                 "
          + "    where                                                                                                                                                              "
          + "      structured_result_type_key in                                                                                                                                     "
          + "      (                                                                                                                                                                "
+" 16504"
+",16509"
+",34558"
+",16511"
+",16512"
+",16525"
+",16529"
+",34559"
+",16519"
+",16528"
+",16531"
+",16532"
+",34560"
+",16539"
+",16540"
+",16546"
+",16549"
+",34561"
+",16556"
+",16551"
+",16515"
+",16517"
+",16523"
+",16530"
+",16541"
+",16544"
+",16561"
+",16554"
          + "      )                                                                                                                                                                "
          + "  ) lsrt                                                                                                                                                               "
          + "  left outer join proximate_lab_result_keys plrk on (plrk.structured_result_type_key = lsrt.structured_result_type_key)                                                "
          + "  left outer join ehcvw.fact_result_lab frl on (frl.result_lab_key = plrk.result_lab_key)                                                                              "
          + "  left outer join ehcvw.lkp_unit_measure lum on (lum.unit_measure_key = frl.unit_measure_key)                                                                          "
          + "  left outer join ehcvw.lkp_result_interpretation lri on (lri.result_interpretation_key = frl.result_interpretation_key)                                               "
          + "order by                                                                                                                                                               "
          + "  decode                                                                                                                                                               "
          + "  (                                                                                                                                                                    "
          + "    lsrt.structured_result_type_desc                                                                                                                                   "
+",'Nucleated Count CSF Tube 1'             ,  1"
+",'Rbc Count CSF Tube 1'                   ,  2"
+",'Neutrophil CSF Tube 1'                  ,  3"
+",'Lymphocyte CSF Tube 1'                  ,  4"
+",'Macrophage CSF Tube 1'                  ,  5"
+",'Other Cells CSF Tube 1'                 ,  6"
+",'Number Cells Counted On CSF Diff Tube 1',  7"
+",'Nucleated Count CSF Tube 2'             ,  8"
+",'Rbc Count CSF Tube 2'                   ,  9"
+",'Neutrophil CSF Tube 2'                  , 10"
+",'Lymphocyte CSF Tube 2'                  , 11"
+",'Macrophage CSF Tube 2'                  , 12"
+",'Other Cells CSF Tube 2'                 , 13"
+",'Number Cells Counted On CSF Diff Tube 2', 14"
+",'Nucleated Count CSF Tube 3'             , 15"
+",'Rbc Count CSF Tube 3'                   , 16"
+",'Neutrophil CSF Tube 3'                  , 17"
+",'Lymphocyte CSF Tube 3'                  , 18"
+",'Macrophage CSF Tube 3'                  , 19"
+",'Other Cells CSF Tube 3'                 , 20"
+",'Number Cells Counted On CSF Diff Tube 3', 21"
+",'Nucleated Count CSF Tube 4'             , 22"
+",'Rbc Count CSF Tube 4'                   , 23"
+",'Neutrophil CSF Tube 4'                  , 24"
+",'Lymphocyte CSF Tube 4'                  , 25"
+",'Macrophage CSF Tube 4'                  , 26"
+",'Other Cells CSF Tube 4'                 , 27"
+",'Number Cells Counted On CSF Diff Tube 4', 28"
          + "  )                                                                                                                                                                    "
        );       
    }

    public List<PathNetResult> getPathNetResultsByEmpiProximateToCollectionDate(String empi, Date collectionDate) {
        List<PathNetResult> pathNetResults = new ArrayList<>();
        try {
            pstmt1.setDate(1, collectionDate);
            pstmt1.setDate(2, collectionDate);
            pstmt1.setString(3, empi);
            pstmt1.setDate(4, collectionDate);
            pstmt1.setDate(5, collectionDate);
            pstmt1.setDate(6, collectionDate);
            ResultSet rs1 = pstmt1.executeQuery();
            while(rs1.next()) {
                pathNetResults.add(new PathNetResult(rs1));
            }
            rs1.close();
        }
        catch(SQLException e) {
            System.out.println(String.format("error getting resutls for EMPI %s and collection date %s", empi, collectionDate));
            e.printStackTrace();
        }
        return pathNetResults;
    }
    
}
