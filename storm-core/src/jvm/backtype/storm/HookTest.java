package backtype.storm;

import backtype.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by pbrahmbhatt on 11/25/15.
 */
public class HookTest implements ISubmitterHook {
    private static final Logger LOG = LoggerFactory.getLogger(HookTest.class);
    @Override
    public void notify(String name, Map stormConf, StormTopology topology) throws IllegalAccessException {
        LOG.info("Executing SubmitterHook for topology " + name);
    }
}
