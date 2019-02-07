package com.serverless;

import java.util.Collections;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.embulk.EmbulkEmbed;
import org.embulk.output.RedshiftOutputPlugin;
import org.embulk.input.RedshiftInputPlugin;

public class Handler implements RequestHandler<Map<String, Object>, Object> {

    private static final Logger LOG = Logger.getLogger(Handler.class);

    @Override
    public Object handleRequest(Map<String, Object> input, Context context) {
        return "OK";
    }
}
