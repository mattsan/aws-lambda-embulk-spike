package com.serverless;

import java.util.Collections;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.inject.Module;
import com.google.inject.Binder;
import org.apache.log4j.Logger;
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

    private static final Logger LOG = Logger.getLogger(Handler.class);

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
            ConfigLoader loader = embulk.newConfigLoader();
            ConfigSource config = loader.fromYamlString(
              "in:\n" +
              "  type: redshift\n" +
              "  host: 転送元のホスト名\n" +
              "  user: 転送元のユーザ名\n" +
              "  password: 転送元のパスワード\n" +
              "  database: 転送元のデータベース名\n" +
              "  table: 転送元のテーブル名\n" +
              "  select: 'id, name, age'\n" + // 転送するカラム
              "  fetch_rows: 1000\n" +
              "out:\n" +
              "  type: redshift\n" +
              "  aws_auth_method: basic\n" + // basic = キーを指定する
              "  access_key_id: アクセスキー\n" +
              "  secret_access_key: シークレットキー\n" +
              "  host: 転送先のホスト名\n" +
              "  user: 転送先のユーザ名\n" +
              "  password: 転送先のパスワード\n" +
              "  database: 転送先のデータベース名\n" +
              "  table: 転送先のテーブル名\n" +
              "  mode: merge\n" +
              "  merge_keys: ['id']\n" +
              "  s3_bucket: テンポラリファイルを格納するバケット\n" +
              "  s3_key_prefix: テンポラリファイルの接頭辞\n"
            );
            ExecutionResult result = embulk.run(config);
            resultMessage = result.toString();
        } catch (Exception e) {
            e.printStackTrace();
            resultMessage = e.toString();
        }

        return resultMessage;
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
