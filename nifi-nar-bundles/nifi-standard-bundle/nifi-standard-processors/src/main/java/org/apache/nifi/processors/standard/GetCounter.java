package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.repository.StandardCounterRepository;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

/**
 * Created by onb813 on 11/10/16.
 */

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"counter","tracking","provenance"})
@CapabilityDescription("Can update counter value and can get counter value to use")
@ReadsAttribute(attribute = "counterName", description = "The name of the counter to update/get.")
public class GetCounter extends AbstractProcessor {



    public static final String RESET_RESOLUTION = "true";
    public static final String DONTRESET_RESOLUTION = "false";

    public static final PropertyDescriptor Operation = new PropertyDescriptor.Builder()
            .name("Reset Counter")
            .description("The name of the counter you want to set the value off - supports expression language like ${counterName}")
            .required(true)
            .defaultValue(DONTRESET_RESOLUTION)
            .allowableValues(DONTRESET_RESOLUTION, RESET_RESOLUTION)
            .build();

    public static final PropertyDescriptor CounterName = new PropertyDescriptor.Builder()
            .name("CounterName")
            .description("The name of the counter you want to get the value off - supports expression language like ${counterName}")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Counter was retrieved")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Counter was not retrieved")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context)
    {
        // Create the Property descriptors.
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(Operation);
        descriptors.add(CounterName);
        this.descriptors = Collections.unmodifiableList(descriptors);

        // Create the Relationships.
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        //TODO this would be for the get part of the processor
        //StandardCounterRepository counterRepository = new StandardCounterRepository();
        //counterRepository.getCounters();

        final ComponentLog logger = getLogger();
        FlowFile flowFile = session.create();
        try
        {
            String name = context.getProperty(CounterName).evaluateAttributeExpressions(flowFile).getValue();
            Long counterVal;
            StandardCounterRepository counterRepository = new StandardCounterRepository();
            List<Counter> counters = counterRepository.getCounters();
            logger.warn("Counters"+counters.toString());
            boolean foundName = false;

            if(name != null && !"".equals(name))
            {
                flowFile = session.putAttribute(flowFile, "Test" ,"test");
                for(Counter counter: counters)
                {
                    logger.warn("CONTEXT: "+counter.getContext());
                    logger.warn("NAME" + counter.getName());
                    logger.warn("value"+ counter.getValue());
                    logger.warn("identifier"+counter.getIdentifier());
                    if(counter.getName().equals(name))
                    {
                        flowFile = session.putAttribute(flowFile, "Test2" ,"test2");
                        counterVal = counter.getValue();
                        flowFile = session.putAttribute(flowFile, "Context" ,counter.getContext());
                        flowFile = session.putAttribute(flowFile, "Identifier" ,counter.getIdentifier());
                        flowFile = session.putAttribute(flowFile, "Name" ,counter.getName());
                        flowFile = session.putAttribute(flowFile, "Value" ,counter.getValue()+"");
                        foundName = true;
                        if("true".equals(context.getProperty(Operation).getValue())) {
                            counterRepository.resetCounter(name);
                        }
                        session.transfer(flowFile,SUCCESS);
                        break;
                    }
                }
                if(!foundName)
                {
                    flowFile = session.putAttribute(flowFile, "Test3" ,"test3");
                    session.transfer(flowFile,FAILURE);
                }
            }

            else
            {
                flowFile = session.putAttribute(flowFile, "Test4" ,"test4");
                if("true".equals(context.getProperty(Operation).getValue())) {
                    flowFile = session.putAttribute(flowFile, "Test5" ,"test5");
                    for (Counter counter : counters) {
                        counterRepository.resetCounter(counter.getName());
                    }
                    session.transfer(flowFile, SUCCESS);
                }
            }

        }
        catch (Exception e)
        {
            flowFile = session.putAttribute(flowFile, "Test5" ,"test5");
            logger.error("The following exception occurred" + e.getMessage());
            session.transfer(flowFile,FAILURE);
        }
    }
}

