package com.manywho.services.backend.services;

import com.manywho.sdk.entities.draw.elements.type.*;

public class BindingService {
    public TypeElement generateBinding(TypeElement typeElement) throws Exception {
        if (typeElement == null) {
            throw new Exception("The TypeElement object cannot be null.");
        }

        if (typeElement.getDeveloperName() == null ||
                typeElement.getDeveloperName().isEmpty() == true) {
            throw new Exception("The TypeElement.DeveloperName property cannot be null or blank when generating a binding.");
        }

        if (typeElement.getProperties() == null ||
                typeElement.getProperties().size() == 0) {
            throw new Exception("The TypeElement.Properties property cannot be null or empty. You cannot generate a binding if you don't have any properties.");
        }

        if (typeElement.getServiceElementId() == null ||
                typeElement.getServiceElementId().isEmpty() == true) {
            throw new Exception("The TypeElement.ServiceElementId property cannot be null or blank. This is needed so the service can bind fully to itself.");
        }

        TypeElementBinding typeElementBinding = new TypeElementBinding();
        typeElementBinding.setDeveloperName(typeElement.getDeveloperName() + " Binding");
        typeElementBinding.setDeveloperSummary("The automatic binding created for " + typeElement.getDeveloperName());
        typeElementBinding.setDatabaseTableName(this.generateSafeName(typeElement.getDeveloperName()));
        typeElementBinding.setPropertyBindings(new TypeElementPropertyBindingCollection());
        typeElementBinding.setServiceElementId(typeElement.getServiceElementId());

        // Convert each of the properties over to bindings
        for (TypeElementProperty typeElementProperty : typeElement.getProperties()) {
            TypeElementPropertyBinding typeElementPropertyBinding = new TypeElementPropertyBinding();
            typeElementPropertyBinding.setTypeElementPropertyId(typeElementProperty.getId());
            typeElementPropertyBinding.setDatabaseContentType(typeElementProperty.getContentType().toString());
            typeElementPropertyBinding.setDatabaseFieldName(this.generateSafeName(typeElementProperty.getDeveloperName()));

            typeElementBinding.getPropertyBindings().add(typeElementPropertyBinding);
        }

        // Assign the binding to the type - removing any existing bindings
        typeElement.setBindings(new TypeElementBindingCollection());
        typeElement.getBindings().add(typeElementBinding);

        // Remove the service element id from the Type as we don't want it to be managed by this service (as it will be
        // deleted on a service refresh and also not be editable in the draw tool
        typeElement.setServiceElementId(null);

        return typeElement;
    }

    private String generateSafeName(String name) throws Exception {
        if (name == null ||
                name.isEmpty() == true) {
            throw new Exception("The provided name cannot be made safe as it is null or empty.");
        }

        // Replace all funny or blank characters with an underscore
        name = name.replaceAll("[^A-Za-z0-9]", "_");

        // Make all of the characters lower case
        name = name.toLowerCase();

        return name;
    }

    public void validateName(String name) throws Exception {
        String pattern = "^[a-z0-9_]*$";

        if (name.matches(pattern) == false) {
            throw new Exception("The provided name is not valid. The name causing the fault is: " + name);
        }
    }
}
