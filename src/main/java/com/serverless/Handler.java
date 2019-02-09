package com.serverless;

import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.inject.Module;
import com.google.inject.Binder;
import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.exec.ExecutionResult;
import org.embulk.input.RedshiftInputPlugin;
import org.embulk.output.RedshiftOutputPlugin;
import org.embulk.plugin.InjectedPluginSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;

public class Handler implements RequestHandler<Map<String, Object>, Object> {
    @Override
    public Object handleRequest(Map<String, Object> input, Context context) {
        EmbulkEmbed.Bootstrap bootstrap = new EmbulkEmbed.Bootstrap();
        bootstrap.addModules(
          new RedshiftInputModule(),
          new RedshiftOutputModule()
        );

        EmbulkEmbed embulk = bootstrap.initializeCloseable();

        String resultMessage = "resultMessage";

        try {
            String configYaml = readYaml("/config/timeline_logs.yml");
            ConfigLoader loader = embulk.newConfigLoader();
            ConfigSource config = loader.fromYamlString(configYaml);
            ExecutionResult result = embulk.run(config);
            resultMessage = result.toString();
        } catch (Exception e) {
            e.printStackTrace();
            resultMessage = e.toString();
        }

        return resultMessage;
    }

    String readYaml(String path) throws IOException {
        URL url = Handler.class.getResource(path);
        BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

        StringBuffer buffer = new StringBuffer("");
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            buffer.append(inputLine);
            buffer.append("\n");
        }
        in.close();

        return buffer.toString();
    }

    static class RedshiftInputModule implements Module {
        @Override
        public void configure(Binder binder) {
            InjectedPluginSource.registerPluginTo(binder, InputPlugin.class, "redshift", RedshiftInputPlugin.class);
        }
    }

    static class RedshiftOutputModule implements Module {
        @Override
        public void configure(Binder binder) {
            InjectedPluginSource.registerPluginTo(binder, OutputPlugin.class, "redshift", RedshiftOutputPlugin.class);
        }
    }
}
