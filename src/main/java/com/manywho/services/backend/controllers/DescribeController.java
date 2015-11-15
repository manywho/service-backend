package com.manywho.services.backend.controllers;

import com.manywho.sdk.entities.describe.DescribeServiceResponse;
import com.manywho.sdk.entities.describe.DescribeValue;
import com.manywho.sdk.entities.describe.DescribeValueCollection;
import com.manywho.sdk.entities.run.elements.config.ServiceRequest;
import com.manywho.sdk.entities.draw.elements.type.TypeElement;
import com.manywho.sdk.entities.translate.Culture;
import com.manywho.sdk.enums.ContentType;
import com.manywho.sdk.services.describe.DescribeServiceBuilder;
import com.manywho.services.backend.configuration.Configuration;
import com.manywho.services.backend.services.BindingService;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/")
@Consumes("application/json")
@Produces("application/json")
public class DescribeController {
    @Inject
    private BindingService bindingService;

    @Path("/metadata")
    @POST
    public DescribeServiceResponse describe(ServiceRequest serviceRequest) throws Exception {
        return new DescribeServiceBuilder()
                .setProvidesDatabase(true)
                .setCulture(new Culture("EN", "US"))
                .setConfigurationValues(
                        new DescribeValueCollection() {{
                            add(new DescribeValue(Configuration.DATABASE_URL, ContentType.String, true));
                            add(new DescribeValue(Configuration.DATABASE_USERNAME, ContentType.String, true));
                            add(new DescribeValue(Configuration.DATABASE_PASSWORD, ContentType.Password, true));
                        }}
                )
                .createDescribeService()
                .createResponse();
    }

    @Path("/metadata/binding")
    @POST
    public TypeElement describeBinding(TypeElement typeElement) throws Exception {
        return this.bindingService.generateBinding(typeElement);
    }
}
