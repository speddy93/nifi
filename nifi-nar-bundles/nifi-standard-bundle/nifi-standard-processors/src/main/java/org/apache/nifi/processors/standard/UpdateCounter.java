package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

/**
 * Created by onb813 on 11/10/16.
 */

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"counter","tracking","provenance"})
@CapabilityDescription("Can update counter value and can get counter value to use")
@ReadsAttribute(attribute = "counterName", description = "The name of the counter to update/get.")
public class UpdateCounter extends AbstractProcessor {



    public static final PropertyDescriptor CounterName = new PropertyDescriptor.Builder()
            .name("CounterName")
            .description("The name of the counter you want to set the value off - supports expression language like ${counterName}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    //TODO Can this support expression language too?
    public static final PropertyDescriptor Delta = new PropertyDescriptor.Builder()
            .name("DELTA")
            .description("If positive increments counter by delta - if negative decrements counter by delta")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();


    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Counter was updated/retrieved")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Counter was not updated/retrieved")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;



    @Override
    protected void init(final ProcessorInitializationContext context)
    {
        // Create the Property descriptors.
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CounterName);
        descriptors.add(Delta);
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
        FlowFile flowFile = session.get();
        try {
            //I Can also do checks for positive and negative -- prob not needed
            session.adjustCounter(context.getProperty(CounterName).evaluateAttributeExpressions(flowFile).getValue(), Long.parseLong(context.getProperty(Delta).getValue()), false);
            session.transfer(flowFile, SUCCESS);
        } catch (Exception e) {
            logger.error("The following exception occurred" + e.getMessage());
            session.transfer(flowFile, FAILURE);
        }

    }
}
