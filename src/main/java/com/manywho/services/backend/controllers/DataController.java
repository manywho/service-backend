package com.manywho.services.backend.controllers;

import com.manywho.sdk.entities.run.elements.type.ObjectDataRequest;
import com.manywho.sdk.entities.run.elements.type.ObjectDataResponse;
import com.manywho.sdk.services.controllers.AbstractDataController;
import com.manywho.services.backend.configuration.Configuration;
import com.manywho.services.backend.services.DataService;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/")
@Consumes("application/json")
@Produces("application/json")
public class DataController extends AbstractDataController {
    @Inject
    private DataService dataService;

    @Override
    public ObjectDataResponse delete(ObjectDataRequest objectDataRequest) throws Exception {
        throw new Exception("The DELETE operation has not yet been implemented.");
    }

    @Override
    public ObjectDataResponse load(ObjectDataRequest objectDataRequest) throws Exception {
        return this.dataService.load(
                this.getAuthenticatedWho(),
                this.parseConfigurationValues(objectDataRequest, Configuration.class),
                objectDataRequest
        );
    }

    @Override
    public ObjectDataResponse save(ObjectDataRequest objectDataRequest) throws Exception {
        return this.dataService.save(
                this.getAuthenticatedWho(),
                this.parseConfigurationValues(objectDataRequest, Configuration.class),
                objectDataRequest
        );
    }
}
