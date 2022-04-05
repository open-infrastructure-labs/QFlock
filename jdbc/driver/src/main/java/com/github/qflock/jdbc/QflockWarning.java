package com.github.qflock.jdbc;

import java.sql.SQLWarning;
import java.util.List;

import com.github.qflock.jdbc.api.QFSQLWarning;

public class QflockWarning {

    public static SQLWarning buildFromList(List<QFSQLWarning> warnings) {
        if (warnings==null){
            return null;
        }
        if (warnings.isEmpty()){
            return null;
        }
        SQLWarning ret_warn = buildFromPart(warnings.get(0));
        SQLWarning next = ret_warn;
        for (int i = 1; i < warnings.size(); i++) {
            next.setNextWarning(buildFromPart(warnings.get(i)));
            next = next.getNextWarning();
        }
        return ret_warn;
    }
    public static SQLWarning buildFromPart(QFSQLWarning warning) {
        return new SQLWarning(warning.reason, warning.state, warning.vendorCode);
    }

}
