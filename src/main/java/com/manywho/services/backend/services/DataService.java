package com.manywho.services.backend.services;

import com.manywho.sdk.entities.run.elements.type.ObjectCollection;
import com.manywho.sdk.entities.run.elements.type.ObjectDataRequest;
import com.manywho.sdk.entities.run.elements.type.ObjectDataResponse;
import com.manywho.sdk.entities.security.AuthenticatedWho;
import com.manywho.services.backend.configuration.Configuration;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;

public class DataService {
    @Inject
    private DatabaseService databaseService;

    public ObjectDataResponse load(AuthenticatedWho authenticatedWho, Configuration configuration, ObjectDataRequest objectDataRequest) throws Exception {
        if (authenticatedWho == null) {
            throw new Exception(("The AuthenticatedWho object cannot be null."));
        }

        if (configuration == null) {
            throw new Exception(("The Configuration object cannot be null."));
        }

        if (objectDataRequest == null) {
            throw new Exception("The ObjectDataRequest object cannot be null.");
        }

        // Construct the object data response as we should always return that unless there are errors
        ObjectDataResponse objectDataResponse = new ObjectDataResponse();
        objectDataResponse.setCulture(objectDataRequest.getCulture());

        Connection connection = null;

        try {
            // Construct and execute the query on the backend
            connection = this.databaseService.getConnection(configuration);

            // Set the object data based on the object data request
            objectDataResponse.setObjectData(this.databaseService.executeObjectLoad(authenticatedWho, connection, objectDataRequest));
        } catch (SQLException e) {
            throw e;
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw e;
            }
        }

        return objectDataResponse;
    }

    public ObjectDataResponse save(AuthenticatedWho authenticatedWho, Configuration configuration, ObjectDataRequest objectDataRequest) throws Exception {
        if (authenticatedWho == null) {
            throw new Exception(("The AuthenticatedWho object cannot be null."));
        }

        if (configuration == null) {
            throw new Exception(("The Configuration object cannot be null."));
        }

        if (objectDataRequest == null) {
            throw new Exception("The ObjectDataRequest object cannot be null.");
        }

        if (objectDataRequest.getObjectDataType() == null) {
            throw new Exception("The ObjectDataRequest.ObjectDataType information is null. As a result, the save request does not have enough information to execute.");
        }

        if (objectDataRequest.getObjectDataType().getDeveloperName() == null ||
                objectDataRequest.getObjectDataType().getDeveloperName().isEmpty() == true) {
            throw new Exception("The ObjectDataRequest.ObjectDataType.DeveloperName must be provided. This property is used to determine the type of data to be loaded.");
        }

        if (objectDataRequest.getObjectDataType().getProperties() == null ||
                objectDataRequest.getObjectDataType().getProperties().size() == 0) {
            throw new Exception("The ObjectDataRequest.ObjectDataType.Properties must be provided. This property is provides the fields that are included in the data to be loaded.");
        }

        // Construct the object data response as we should always return that unless there are errors
        ObjectDataResponse objectDataResponse = new ObjectDataResponse();
        objectDataResponse.setCulture(objectDataRequest.getCulture());
        objectDataResponse.setObjectData(new ObjectCollection());

        // If we don't have any object data to save, we simply return an empty response
        if (objectDataRequest.getObjectData() == null ||
                objectDataRequest.getObjectData().size() == 0) {
            return objectDataResponse;
        }

        Connection connection = null;

        try {
            // Construct and execute the query on the backend
            connection = this.databaseService.getConnection(configuration);

            // Execute the save across all objects in the hierarchy
            this.databaseService.executeObjectSave(authenticatedWho, connection, null, objectDataRequest.getObjectData());
        } catch (SQLException e) {
            throw e;
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw e;
            }
        }

        return objectDataResponse;
    }
}
