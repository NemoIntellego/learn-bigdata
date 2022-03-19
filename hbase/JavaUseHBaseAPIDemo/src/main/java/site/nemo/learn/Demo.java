package site.nemo.learn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static site.nemo.learn.HBaseUtil.*;

public class Demo {

    private static Logger logger = LoggerFactory.getLogger(Demo.class);

    public static void main(String[] args) throws IOException {
        String tableName = "nemo:student";

        if (isTableExist(tableName)) {
            dropTable(tableName);
        }

        createTable(tableName, "info", "score");
        logger.info("----create table done----");

        putData(tableName, "Tom", "info", "student_id", "20210000000001");
        putData(tableName, "Tom", "info", "class", "1");
        putData(tableName, "Tom", "score", "understanding", "75");
        putData(tableName, "Tom", "score", "programming", "82");
        putData(tableName, "Jerry", "info", "student_id", "20210000000002");
        putData(tableName, "Jerry", "info", "class", "1");
        putData(tableName, "Jerry", "score", "understanding", "85");
        putData(tableName, "Jerry", "score", "programming", "67");
        putData(tableName, "Jack", "info", "student_id", "20210000000003");
        putData(tableName, "Jack", "info", "class", "2");
        putData(tableName, "Jack", "score", "understanding", "80");
        putData(tableName, "Jack", "score", "programming", "80");
        putData(tableName, "Rose", "info", "student_id", "20210000000004");
        putData(tableName, "Rose", "info", "class", "2");
        putData(tableName, "Rose", "score", "understanding", "60");
        putData(tableName, "Rose", "score", "programming", "61");
        putData(tableName, "Nemo", "info", "student_id", "20210000000005");
        logger.info("----put data done----");

        getData(tableName, "Nemo", "info", "student_id");
        logger.info("----get data done----");

        scanTable(tableName);
        logger.info("----scan table done----");
    }
}
