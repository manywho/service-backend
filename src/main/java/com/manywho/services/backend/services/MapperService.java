package com.manywho.services.backend.services;

import com.manywho.sdk.entities.run.elements.type.*;
import com.manywho.sdk.entities.run.elements.type.Object;
import org.json.JSONObject;

import javax.inject.Inject;

public class MapperService {
    @Inject
    private BindingService bindingService;

    @Inject
    private DatabaseService databaseService;

    public com.manywho.sdk.entities.run.elements.type.Object convertJSONObjectToObject(String name, String externalId, JSONObject jsonObject) throws Exception {
        if (name == null ||
                name.isEmpty() == true) {
            throw new Exception("The name of the object cannot be null or blank.");
        }

        // Validate the object name is valid
        this.bindingService.validateName(name);

        if (externalId == null ||
                externalId.isEmpty() == true) {
            throw new Exception("The external identifier for the object cannot be null or blank.");
        }

        // Validate the external identifier
        this.databaseService.validateUUID(externalId);

        Object object = null;

        if (jsonObject != null &&
                jsonObject.keys() != null) {
            // Construct the object that will hold the record data
            object = new Object();
            object.setDeveloperName(name);
            object.setExternalId(externalId);
            object.setProperties(new PropertyCollection());

            // Go through each of the keys in the json object and grab out the data
            while (jsonObject.keys().hasNext()) {
                String key = (String) jsonObject.keys().next();

                // Add each property individually. For now we assume all properties are strings
                object.getProperties().add(
                        new Property(
                                key,
                                jsonObject.getString(key)
                        )
                );
            }
        }

        return object;
    }
}
