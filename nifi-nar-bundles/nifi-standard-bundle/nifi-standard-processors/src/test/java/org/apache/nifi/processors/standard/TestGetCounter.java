package org.apache.nifi.processors.standard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.nifi.controller.Counter;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
/**
 * Created by onb813 on 11/15/16.
 */
public class TestGetCounter {


    @Test
    public void testwithFileName() throws Exception {
        final TestRunner firstrunner = TestRunners.newTestRunner(new UpdateCounter());
        firstrunner.setProperty(UpdateCounter.CounterName,"ironport");
        firstrunner.setProperty(UpdateCounter.Delta,"1");
        Map<String,String> attributes = new HashMap<String,String>();
        firstrunner.enqueue("",attributes);
        firstrunner.run();
        Long counter = firstrunner.getCounterValue("ironport");
        firstrunner.assertAllFlowFilesTransferred(UpdateCounter.SUCCESS, 1);

//        final TestRunner runner = TestRunners.newTestRunner(new GetCounter());
//        runner.setProperty(GetCounter.CounterName, "ironport");
//        runner.setProperty(GetCounter.Operation,GetCounter.DONTRESET_RESOLUTION);
//
//        runner.run();


    }

}
