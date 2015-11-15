package com.manywho.services.backend;

import com.manywho.services.backend.services.DatabaseService;
import com.manywho.services.backend.services.DataService;
import com.manywho.services.backend.services.BindingService;
import com.manywho.services.backend.services.MapperService;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

public class ApplicationBinder extends AbstractBinder {
    @Override
    protected void configure() {
        bind(DataService.class).to(DataService.class);
        bind(BindingService.class).to(BindingService.class);
        bind(DatabaseService.class).to(DatabaseService.class);
        bind(MapperService.class).to(MapperService.class);
    }
}
