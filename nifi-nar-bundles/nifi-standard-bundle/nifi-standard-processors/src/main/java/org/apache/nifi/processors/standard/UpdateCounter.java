/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

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
    protected void init(final ProcessorInitializationContext context) {
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
