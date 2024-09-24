package com.fitbank.loan.maintenance;

import com.fitbank.common.ApplicationDates;
import com.fitbank.common.Helper;
import com.fitbank.common.exception.FitbankException;
import com.fitbank.common.hb.UtilHB;
import com.fitbank.common.helper.Constant;
import com.fitbank.dto.management.Detail;
import com.fitbank.hb.persistence.acco.loan.Tloanaccount;
import com.fitbank.hb.persistence.acco.loan.TloanaccountKey;
import com.fitbank.hb.persistence.acco.loan.Tquotasaccount;
import com.fitbank.hb.persistence.acco.loan.TquotasaccountKey;
import com.fitbank.hb.persistence.acco.loan.Tquotascategoriesaccount;
import com.fitbank.hb.persistence.gene.Tsystemparametercompany;
import com.fitbank.hb.persistence.gene.TsystemparametercompanyKey;
import com.fitbank.processor.maintenance.MaintenanceCommand;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.RoundingMode;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SQLQuery;

/**
 *
 * @author matprog04
 */
@Slf4j
public class UpdateInsuranceFlatQ extends MaintenanceCommand {

    private final String HQL_SOL_CUO = " from Tquotascategoriesaccount tsq where tsq.pk.ccuenta=:ccuenta"
            + " and tsq.pk.categoria='SEGDLI' and tsq.pk.fhasta=:fncfhasta order by tsq.pk.subcuenta";

    private final String HQL_SOL_CUO_1 = " from Tquotascategoriesaccount tsq where tsq.pk.ccuenta=:ccuenta"
            + " and tsq.pk.categoria='SEGDMA' and tsq.pk.fhasta=:fncfhasta order by tsq.pk.subcuenta";

    String setccuenta = "";
    int linea = 1;

    @Override
    public Detail executeNormal(Detail pDetail) throws Exception {

        String proceso = pDetail.findFieldByNameCreate("PROCESO").getStringValue();
        String seg = pDetail.findFieldByNameCreate("SEGURO").getStringValue();

        if ("E".equals(proceso)) {
            String path = this.getParameter();
            System.out.println(path);
            String filename = (String) pDetail.findFieldByNameCreate("ARCHIVO").getValue();
            if (filename == null) {
                throw new FitbankException("GEN026", "CAMPOS NO DEFINIDOS");
            }
            File file = new File(path + filename);
            if (file.exists()) {
                String line;
                BufferedReader br = new BufferedReader(new FileReader(file));
                while ((line = br.readLine()) != null) {

                    String[] splitLine2 = line.split(";");
                    setccuenta = splitLine2[0];
                    if (seg.equals("SEGDLI")) {
                        UtilHB utilHB = new UtilHB();
                        utilHB.setSentence(HQL_SOL_CUO);
                        utilHB.setString("ccuenta", setccuenta);
                        utilHB.setTimestamp("fncfhasta", ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
                        List<Tquotascategoriesaccount> tcsol = utilHB.getList(false);

                        for (Tquotascategoriesaccount t : tcsol) {
                            TquotasaccountKey tquotasaccountKey = new TquotasaccountKey(setccuenta, t.getPk().getSubcuenta(), 0,
                                    t.getPk().getFparticion(), ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP, pDetail.getCompany());
                            Tquotasaccount tquotasaccount = (Tquotasaccount) Helper
                                    .getBean(Tquotasaccount.class, tquotasaccountKey);

                            t.setValorcategoria(tquotasaccount.getSeguro());
                            t.setValordeudorcategoria(tquotasaccount.getSeguro());
                            Helper.update(t);
                        }
                    } else {
                        UtilHB utilHB = new UtilHB();
                        utilHB.setSentence(HQL_SOL_CUO_1);
                        utilHB.setString("ccuenta", setccuenta);
                        utilHB.setTimestamp("fncfhasta", ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
                        List<Tquotascategoriesaccount> tcsol = utilHB.getList(false);

                        for (Tquotascategoriesaccount t : tcsol) {
                            TquotasaccountKey tquotasaccountKey = new TquotasaccountKey(setccuenta, t.getPk().getSubcuenta(), 0,
                                    t.getPk().getFparticion(), ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP, pDetail.getCompany());
                            Tquotasaccount tquotasaccount = (Tquotasaccount) Helper
                                    .getBean(Tquotasaccount.class, tquotasaccountKey);

                            t.setValorcategoria(tquotasaccount.getSeguro());
                            t.setValordeudorcategoria(tquotasaccount.getSeguro());
                            Helper.update(t);
                        }
                    }

                    linea++;
                    log.info("Actualizacion de la tabla tcuentacuotascategorias realizada al credito: " + setccuenta);

                }
                br.close();
                //file.delete();
            } else {
                throw new FitbankException("GEN026", "CAMPOS NO DEFINIDOS");
            }
        }
        return pDetail;

    }

    @Override
    public Detail executeReverse(Detail detail) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
