package com.serverless;

import java.util.Collections;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.inject.Module;
import com.google.inject.Binder;
import org.apache.log4j.Logger;
import org.embulk.EmbulkEmbed;
import org.embulk.input.RedshiftInputPlugin;
import org.embulk.output.RedshiftOutputPlugin;
import org.embulk.plugin.InjectedPluginSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;

public class Handler implements RequestHandler<Map<String, Object>, Object> {

    private static final Logger LOG = Logger.getLogger(Handler.class);

    @Override
    public Object handleRequest(Map<String, Object> input, Context context) {
        EmbulkEmbed.Bootstrap bootstrap = new EmbulkEmbed.Bootstrap();
        bootstrap.addModules(
          new RedshiftInputModule(),
          new RedshiftOutputModule()
        );

        EmbulkEmbed embulk = bootstrap.initializeCloseable();
        return "Ya!";
    }

    static class RedshiftInputModule implements Module {
        @Override
        public void configure(Binder binder) {
            InjectedPluginSource.registerPluginTo(binder, InputPlugin.class, "redfshiftInput", RedshiftInputPlugin.class);
        }
    }

    static class RedshiftOutputModule implements Module {
        @Override
        public void configure(Binder binder) {
            InjectedPluginSource.registerPluginTo(binder, OutputPlugin.class, "redfshiftOutput", RedshiftOutputPlugin.class);
        }
    }
}
