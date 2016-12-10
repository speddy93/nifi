package org.apache.nifi.processors.standard;

import java.util.*;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
/**
 * Created by onb813 on 11/15/16.
 */
public class TestUpdateCounter {


    @Test
    public void testwithFileName() throws Exception {
        final TestRunner firstrunner = TestRunners.newTestRunner(new UpdateCounter());
        firstrunner.setProperty(UpdateCounter.CounterName,"firewall");
        firstrunner.setProperty(UpdateCounter.Delta,"1");
        Map<String,String> attributes = new HashMap<String,String>();
        firstrunner.enqueue("",attributes);
        firstrunner.run();
        Long counter = firstrunner.getCounterValue("firewall");
        firstrunner.assertAllFlowFilesTransferred(UpdateCounter.SUCCESS, 1);
    }

}