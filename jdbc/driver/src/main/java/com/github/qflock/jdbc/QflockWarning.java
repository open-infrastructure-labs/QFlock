/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
