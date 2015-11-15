package com.manywho.services.backend.services;

import com.manywho.sdk.entities.run.elements.type.*;
import com.manywho.sdk.entities.run.elements.type.Object;
import com.manywho.sdk.entities.security.AuthenticatedWho;
import com.manywho.sdk.enums.CriteriaType;
import com.manywho.services.backend.configuration.Configuration;
import org.json.JSONObject;

import java.util.UUID;
import java.sql.*;

public class DataService {
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
            connection = getConnection(configuration);

            // Set the object data based on the object data request
            objectDataResponse.setObjectData(executeObjectLoad(authenticatedWho, connection, objectDataRequest));
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
            connection = getConnection(configuration);

            // Execute the save across all objects in the hierarchy
            executeObjectSave(authenticatedWho, connection, null, objectDataRequest.getObjectData());
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

    private ObjectCollection executeObjectSave(AuthenticatedWho authenticatedWho, Connection connection, String parentId, ObjectCollection objects) throws Exception {
        if (authenticatedWho == null) {
            throw new Exception(("The AuthenticatedWho object cannot be null."));
        }

        if (connection == null) {
            throw new Exception("The Connection object cannot be null.");
        }

        if (connection.isClosed() == true) {
            throw new Exception("The Connection object is currently closed. As a result, the objects cannot be saved.");
        }

        if (authenticatedWho.getManyWhoTenantId() == null ||
                authenticatedWho.getManyWhoTenantId().isEmpty() == true) {
            throw new Exception("The AuthenticatedWho.ManyWhoTenantId cannot be null or blank.");
        }

        ObjectCollection objectCollection = null;

        // Make sure we have some objects to save
        if (objects != null &&
                objects.size() > 0) {
            // We have objects to save
            objectCollection = new ObjectCollection();

            // Go through the object data and convert to a json object
            for (Object object : objects) {
                JSONObject jsonObject = new JSONObject();
                Object existingObject = null;

                // Check to see if this is an existing object
                if (object.getExternalId() != null &&
                        object.getExternalId().isEmpty() == false) {
                    try {
                        // Check to make sure the external identifier is a valid UUID
                        UUID.fromString(object.getExternalId());
                    } catch (IllegalArgumentException e) {
                        throw new Exception("The provided external identifier for an object to be saved is invalid.");
                    }

                    // Construct an object data request to load this specific object from the backend
                    ObjectDataRequest objectDataRequest = new ObjectDataRequest();
                    objectDataRequest.setObjectDataType(new ObjectDataType());
                    objectDataRequest.getObjectDataType().setDeveloperName(object.getDeveloperName());
                    objectDataRequest.setListFilter(new ListFilter());
                    objectDataRequest.getListFilter().setId(object.getExternalId());

                    // We also load the existing object as we'll need that for data integrity reasons
                    ObjectCollection existingObjectCollection = executeObjectLoad(authenticatedWho, connection, objectDataRequest);

                    if (existingObjectCollection != null &&
                            existingObjectCollection.size() > 0) {
                        if (existingObjectCollection.size() > 1) {
                            throw new Exception("The provided external identifier returns more than one result. The identifier is: '" + object.getExternalId() + "'");
                        }

                        // Get the object from the collection
                        existingObject = existingObjectCollection.get(0);
                    }
                } else {
                    // Assign an id for this object as it's new
                    object.setExternalId(UUID.randomUUID().toString());
                }

                // Go through all of the properties and assign them into json object or save complex properties as child records
                for (Property property : object.getProperties()) {
                    if (property.getDeveloperName() == null ||
                            property.getDeveloperName().isEmpty() == true) {
                        throw new Exception("The ObjectDataRequest.ObjectData[].Properties[].DeveloperName must be provided. This property indicates which field is being saved.");
                    }

                    if (property.getObjectData() != null &&
                            property.getObjectData().size() > 0) {
                        // Repeat up the stack of objects as child objects are stored as separate records and are
                        // therefore excluded from the json at this level
                        executeObjectSave(authenticatedWho, connection, object.getExternalId(), property.getObjectData());
                    } else {
                        // Add the value to the json
                        jsonObject.put(property.getDeveloperName(), property.getContentValue());
                    }
                }

                // Before we insert into the database, we need to make sure the incoming request isn't a partial save. This
                // means that properties will only be included if they changed in the workflow. So we need to merge the existing
                // data with the data coming in rather than assuming the incoming data is complete.
                if (existingObject != null &&
                        existingObject.getProperties() != null &&
                        existingObject.getProperties().size() > 0) {
                    for (Property property : existingObject.getProperties()) {
                        // We add this additional check in case there's any form of data corruption, but we don't want to halt
                        // the save as that can get us in a catch 22 situation
                        if (property.getDeveloperName() != null &&
                                property.getDeveloperName().isEmpty() == false) {
                            // Check to see if the key exists
                            if (jsonObject.has(property.getDeveloperName()) == false) {
                                // The property doesn't exist, so we add it
                                jsonObject.put(property.getDeveloperName(), property.getContentValue());
                            }
                        }
                    }
                }

                String sql;

                if (existingObject != null) {
                    sql = "INSERT INTO typetables ";
                    sql += "(id, parentid, tenantid, name, data) ";
                    sql += "VALUES (";
                    sql += "'" + object.getExternalId() + "', ";
                    sql += "'" + parentId + "', ";
                    sql += "'" + authenticatedWho.getManyWhoTenantId() + "', ";
                    sql += "'" + object.getDeveloperName() + "', ";
                    sql += "'" + jsonObject.toString() + "', ";
                } else {
                    // Generate the insert as the object either doesn't exist when it should or never existed in the backend
                    sql = "INSERT INTO typetables ";
                    sql += "(id, parentid, tenantid, name, data) ";
                    sql += "VALUES (";
                    sql += "'" + object.getExternalId() + "', ";
                    sql += "'" + parentId + "', ";
                    sql += "'" + authenticatedWho.getManyWhoTenantId() + "', ";
                    sql += "'" + object.getDeveloperName() + "', ";
                    sql += "'" + jsonObject.toString() + "', ";
                }

                Statement statement = null;

                try {
                    // Execute the sql statement
                    statement = connection.createStatement();
                    statement.executeUpdate(sql);
                } catch (SQLException e) {
                    throw e;
                } finally {
                    try {
                        if (statement != null) {
                            statement.close();
                        }
                    } catch (SQLException e) {
                        throw e;
                    }
                }

                // Now the object has been saved, we return the fully hydrated object
                objectCollection.add(convertJSONObjectToObject(object.getDeveloperName(), object.getExternalId(), jsonObject));
            }
        }

        return objectCollection;
    }

    private ObjectCollection executeObjectLoad(AuthenticatedWho authenticatedWho, Connection connection, ObjectDataRequest objectDataRequest) throws Exception {
        if (authenticatedWho == null) {
            throw new Exception(("The AuthenticatedWho object cannot be null."));
        }

        if (connection == null) {
            throw new Exception("The Connection object cannot be null.");
        }

        if (connection.isClosed() == true) {
            throw new Exception("The Connection object is currently closed. As a result, the objects cannot be saved.");
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

        ObjectCollection objectCollection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(getSelectStatementForObjectDataRequest(authenticatedWho, objectDataRequest));

            // Go through each record in the result set and convert as per the object data type information
            if (resultSet.next()) {
                // Convert the json object back to a ManyWho object
                Object object = convertJSONObjectToObject(
                        objectDataRequest.getObjectDataType().getDeveloperName(),
                        resultSet.getString(0),
                        new JSONObject(resultSet.getString(1))
                );

                if (object != null) {
                    // Add the object to the collection result
                    objectCollection.add(object);
                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }

                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                throw e;
            }
        }

        return objectCollection;
    }

    private Object convertJSONObjectToObject(String name, String externalId, JSONObject jsonObject) throws Exception {
        if (name == null ||
                name.isEmpty() == true) {
            throw new Exception("The name of the object cannot be null or blank.");
        }

        if (externalId == null ||
                externalId.isEmpty() == true) {
            throw new Exception("The external identifier for the object cannot be null or blank.");
        }

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

    private String getSelectStatementForObjectDataRequest(AuthenticatedWho authenticatedWho, ObjectDataRequest objectDataRequest) throws Exception {
        if (authenticatedWho == null) {
            throw new Exception(("The AuthenticatedWho object cannot be null."));
        }

        if (objectDataRequest == null) {
            throw new Exception("The ObjectDataRequest object cannot be null.");
        }

        if (objectDataRequest.getObjectDataType() == null) {
            throw new Exception("The ObjectDataRequest.ObjectDataType information is null. As a result, the load request does not have enough information to execute.");
        }

        if (objectDataRequest.getObjectDataType().getDeveloperName() == null ||
                objectDataRequest.getObjectDataType().getDeveloperName().isEmpty() == true) {
            throw new Exception("The ObjectDataRequest.ObjectDataType.DeveloperName must be provided. This property is used to determine the type of data to be loaded.");
        }

        if (authenticatedWho.getManyWhoTenantId() == null ||
                authenticatedWho.getManyWhoTenantId().isEmpty() == true) {
            throw new Exception("The AuthenticatedWho.ManyWhoTenantId cannot be null or blank.");
        }

        // Create the select for the json
        String sql = "SELECT id, data FROM typetables ";
        sql += "WHERE name = '" + objectDataRequest.getObjectDataType().getDeveloperName() + "' ";
        sql += "AND tenantid = '" + authenticatedWho.getManyWhoTenantId() + "' ";

        if (objectDataRequest.getListFilter() != null) {
            if (objectDataRequest.getListFilter().getSearch() != null &&
                    objectDataRequest.getListFilter().getSearch().isEmpty() == false) {
                throw new Exception("The ObjectDataRequest.ListFilter.Search property is not supported.");
            }

            if (objectDataRequest.getListFilter().getId() != null &&
                    objectDataRequest.getListFilter().getId().isEmpty() == false) {
                // If loading by identifier, we don't need to do anything else as that's the only required filter
                sql += "AND id = '" + objectDataRequest.getListFilter().getId() + "' ";
            } else {
                // If a comparison hasn't been provided, we assume AND
                if (objectDataRequest.getListFilter().getComparisonType() == null ||
                        objectDataRequest.getListFilter().getComparisonType().isEmpty() == true) {
                    objectDataRequest.getListFilter().setComparisonType("AND");
                }

                // Make sure the limit is valid
                if (objectDataRequest.getListFilter().getLimit() < 1) {
                    throw new Exception("The ObjectDataRequest.ListFilter.Limit cannot be less than 1. A limit of less than 1 will not return any data.");
                }

                // Make sure the offset is valid
                if (objectDataRequest.getListFilter().getOffset() < 0) {
                    throw new Exception("The ObjectDataRequest.ListFilter.Offset cannot be less than 0.");
                }

                if (objectDataRequest.getListFilter().getOrderByDirectionType() != null &&
                        (objectDataRequest.getListFilter().getOrderByDirectionType().equalsIgnoreCase("ASC") == false ||
                         objectDataRequest.getListFilter().getOrderByDirectionType().equalsIgnoreCase("DESC") == false)) {
                    throw new Exception("The ObjectDataRequest.ListFilter.OrderByDirectionType isn't valid. Please provide ASC, DESC or null.");
                }

                if (objectDataRequest.getListFilter().getOrderByPropertyDeveloperName() != null &&
                        objectDataRequest.getListFilter().getOrderByPropertyDeveloperName().isEmpty() == false) {
                    // If an order by direction has not been specified, assume it's ascending
                    if (objectDataRequest.getListFilter().getOrderByDirectionType() == null ||
                            objectDataRequest.getListFilter().getOrderByDirectionType().isEmpty() == true) {
                        objectDataRequest.getListFilter().setOrderByDirectionType("ASC");
                    }
                }

                // Check to see if there are any where clauses specified
                if (objectDataRequest.getListFilter().getWhere() != null &&
                        objectDataRequest.getListFilter().getWhere().size() > 0) {
                    for (ListFilterWhere listFilterWhere : objectDataRequest.getListFilter().getWhere()) {
                        if (listFilterWhere.getColumnName() == null ||
                                listFilterWhere.getColumnName().isEmpty() == true) {
                            throw new Exception("The ObjectDataRequest.ListFilter.Where[].ColumnName must be provided for one of the WHERE entries.");
                        }

                        // If the user has not set a criteria type, we assume EQUAL
                        if (listFilterWhere.getCriteriaType() == null) {
                            listFilterWhere.setCriteriaType(CriteriaType.Equal);
                        }

                        if (listFilterWhere.getContentValue() == null ||
                                listFilterWhere.getContentValue().isEmpty() == true) {

                        }
                    }
                }
            }
        }

        return sql;
    }

    private Connection getConnection(Configuration configuration) throws Exception {
        if (configuration == null) {
            throw new Exception(("The Configuration object cannot be null."));
        }

        if (configuration.getUrl() == null ||
                configuration.getUrl().isEmpty() == true) {
            throw new Exception("The Configuration.Url property cannot be null or blank.");
        }

        if (configuration.getUsername() == null ||
                configuration.getUsername().isEmpty() == true) {
            throw new Exception("The Configuration.Username property cannot be null or blank.");
        }

        if (configuration.getPassword() == null ||
                configuration.getPassword().isEmpty() == true) {
            throw new Exception("The Configuration.Password property cannot be null or blank.");
        }

        return DriverManager.getConnection(configuration.getUrl(), configuration.getUsername(), configuration.getPassword());
    }
}
