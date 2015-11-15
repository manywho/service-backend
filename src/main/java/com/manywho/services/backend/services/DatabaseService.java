package com.manywho.services.backend.services;

import com.manywho.sdk.entities.run.elements.type.*;
import com.manywho.sdk.entities.run.elements.type.Object;
import com.manywho.sdk.entities.security.AuthenticatedWho;
import com.manywho.sdk.enums.CriteriaType;
import com.manywho.services.backend.configuration.Configuration;
import org.json.JSONObject;

import javax.inject.Inject;
import java.sql.*;
import java.util.UUID;

public class DatabaseService {
    @Inject
    private MapperService mapperService;

    @Inject
    private BindingService bindingService;

    public Connection getConnection(Configuration configuration) throws Exception {
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

    public ObjectCollection executeObjectSave(AuthenticatedWho authenticatedWho, Connection connection, String parentId, ObjectCollection objects) throws Exception {
        if (authenticatedWho == null) {
            throw new Exception(("The AuthenticatedWho object cannot be null."));
        }

        if (connection == null) {
            throw new Exception("The Connection object cannot be null.");
        }

        if (connection.isClosed() == true) {
            throw new Exception("The Connection object is currently closed. As a result, the objects cannot be saved.");
        }

        if (parentId != null &&
                parentId.isEmpty() == false) {
            // Validate the external identifier
            this.validateUUID(parentId);
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
            for (com.manywho.sdk.entities.run.elements.type.Object object : objects) {
                JSONObject jsonObject = new JSONObject();
                Object existingObject = null;

                // Validate the object name is valid
                this.bindingService.validateName(object.getDeveloperName());

                // Check to see if this is an existing object
                if (object.getExternalId() != null &&
                        object.getExternalId().isEmpty() == false) {
                    this.validateUUID(object.getExternalId());

                    // Construct an object data request to load this specific object from the backend
                    ObjectDataRequest objectDataRequest = new ObjectDataRequest();
                    objectDataRequest.setObjectDataType(new ObjectDataType());
                    objectDataRequest.getObjectDataType().setDeveloperName(object.getDeveloperName());
                    objectDataRequest.setListFilter(new ListFilter());
                    objectDataRequest.getListFilter().setId(object.getExternalId());

                    // We also load the existing object as we'll need that for data integrity reasons
                    ObjectCollection existingObjectCollection = this.executeObjectLoad(authenticatedWho, connection, objectDataRequest);

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
                    // Validate the property name is valid
                    this.bindingService.validateName(property.getDeveloperName());

                    if (property.getObjectData() != null &&
                            property.getObjectData().size() > 0) {
                        // Repeat up the stack of objects as child objects are stored as separate records and are
                        // therefore excluded from the json at this level
                        this.executeObjectSave(authenticatedWho, connection, object.getExternalId(), property.getObjectData());
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

                PreparedStatement preparedStatement = null;

                try {
                    if (existingObject != null) {
                        // Generate the update as we have the object already
                        preparedStatement = connection.prepareStatement(
                                "UPDATE typetables SET parentid = ?, name = ?, data = ? WHERE id = ? AND tenantid = ?"
                        );
                    } else {
                        // Generate the insert as the object either doesn't exist when it should or never existed in the backend
                        preparedStatement = connection.prepareStatement(
                                "INSERT INTO typetables (parentid, name, data, id, tenantid) VALUES (?, ?, ?, ?, ?)"
                        );
                    }

                    // Add the data
                    preparedStatement.setString(1, parentId);
                    preparedStatement.setString(2, object.getDeveloperName());
                    preparedStatement.setString(3, jsonObject.toString());
                    preparedStatement.setString(4, object.getExternalId());
                    preparedStatement.setString(5, authenticatedWho.getManyWhoTenantId());

                    // Execute the sql statement
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    throw e;
                } finally {
                    try {
                        if (preparedStatement != null) {
                            preparedStatement.close();
                        }
                    } catch (SQLException e) {
                        throw e;
                    }
                }

                // Now the object has been saved, we return the fully hydrated object
                objectCollection.add(
                        this.mapperService.convertJSONObjectToObject(
                                object.getDeveloperName(),
                                object.getExternalId(),
                                jsonObject
                        )
                );
            }
        }

        return objectCollection;
    }

    public ObjectCollection executeObjectLoad(AuthenticatedWho authenticatedWho, Connection connection, ObjectDataRequest objectDataRequest) throws Exception {
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
            resultSet = statement.executeQuery(this.getSelectStatementForObjectDataRequest(authenticatedWho, objectDataRequest));

            // Go through each record in the result set and convert as per the object data type information
            if (resultSet.next()) {
                // Convert the json object back to a ManyWho object
                Object object = this.mapperService.convertJSONObjectToObject(
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

    public void validateUUID(String uuid) throws Exception {
        try {
            // Check to make sure the external identifier is a valid UUID
            UUID.fromString(uuid);
        } catch (IllegalArgumentException e) {
            throw new Exception("The provided identifier is not valid. The value causing this error is: " + uuid);
        }
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

        // Make sure the object name is ok for the select
        this.bindingService.validateName(objectDataRequest.getObjectDataType().getDeveloperName());

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

                        // Validate the column name is OK
                        this.bindingService.validateName(listFilterWhere.getColumnName());

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
}
