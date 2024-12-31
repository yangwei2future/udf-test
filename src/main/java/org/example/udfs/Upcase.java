package org.example.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Upcase extends UDF {
    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }

        // 将字符串转换为大写
        return new Text(s.toString().toUpperCase());
    }
}
