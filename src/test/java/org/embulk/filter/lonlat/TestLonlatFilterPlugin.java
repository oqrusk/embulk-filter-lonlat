package org.embulk.filter.lonlat;

import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.lonlat.LonlatFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.STRING;
import static org.junit.Assert.assertEquals;

public class TestLonlatFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private LonlatFilterPlugin plugin;

    @Before
    public void createResource()
    {
        plugin = new LonlatFilterPlugin();
    }

    private Schema schema(Column... columns)
    {
        return new Schema(Lists.newArrayList(columns));
    }

    private ConfigSource configFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yamlString);
    }

    private PluginTask taskFromYamlString(String... lines)
    {
        ConfigSource config = configFromYamlString(lines);
        return config.loadConfig(PluginTask.class);
    }

    private void transaction(ConfigSource config, Schema inputSchema)
    {
        plugin.transaction(config, inputSchema, new FilterPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
            }
        });
    }

    @Test
    public void test_configure_columns_option()
    {
        PluginTask task = taskFromYamlString(
                "type: lonlat",
                "lat_columns:",
                "  - {name: lat, type: lat, format: MSEC, datum: WGS84}",
                "lon_columns:",
                "  - {name: lon, type: lon, format: MSEC, datum: WGS84}");

        assertEquals(1, task.getLatColumns().size());
        assertEquals(1, task.getLonColumns().size());
    }

    @Test
    public void buildOutputSchema_DropColumns()
    {
        ConfigSource config = configFromYamlString(
                "type: lonlat",
                "lat_columns:",
                "  - {name: lat, type: lat, format: MSEC, datum: WGS84}",
                "lon_columns:",
                "  - {name: lon, type: lon, format: MSEC, datum: WGS84}");

        final Schema inputSchema = Schema.builder()
                .add("lat", DOUBLE)
                .add("lon", DOUBLE)
                .build();

        plugin = new LonlatFilterPlugin();
        plugin.transaction(config, inputSchema, new FilterPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                TestPageBuilderReader.MockPageOutput mockPageOutput = new TestPageBuilderReader.MockPageOutput();
                PageOutput pageOutput = plugin.open(taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                double testLat = 35.6721277;
                double testLon = 139.75891209999997;
                for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(),
                        inputSchema, testLat, "lat")) {
                    pageOutput.add(page);
                }
                for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(),
                        inputSchema, testLon, "lon")) {
                    pageOutput.add(page);
                }

                pageOutput.finish();
                pageOutput.close();

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals(128419659, pageReader.getLong(outputSchema.getColumn(0)));
                    assertEquals(128419659, pageReader.getLong(outputSchema.getColumn(1)));
                }
            }
        });
    }

    @Test
    public void buildOutputSchema_AddColumns()
    {
        PluginTask task = taskFromYamlString(
                "type: column",
                "add_columns:",
                "  - {name: timestamp, type: timestamp, default: 2015-07-13, format: \"%Y-%m-%d\", timezone: UTC}",
                "  - {name: string, type: string, default: string}",
                "  - {name: boolean, type: boolean, default: true}",
                "  - {name: long, type: long, default: 0}",
                "  - {name: double, type: double, default: 0.5}",
                "  - {name: json, type: json, default: \"{\\\"foo\\\":\\\"bar\\\"}\" }");
        Schema inputSchema = Schema.builder()
                .add("keep_me", STRING)
                .build();

        Schema outputSchema = inputSchema;//ColumnFilterPlugin.buildOutputSchema(task, inputSchema);
        assertEquals(7, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("keep_me", column.getName());
        }
        {
            column = outputSchema.getColumn(1);
            assertEquals("timestamp", column.getName());
        }
    }

    @Test(expected = ConfigException.class)
    public void configure_EitherOfColumnsOrDropColumnsCanBeSpecified()
    {
        ConfigSource config = configFromYamlString(
                "type: column",
                "columns:",
                "- {name: a}",
                "drop_columns:",
                "- {name: a}");
        Schema inputSchema = schema(
                new Column(0, "a", STRING),
                new Column(1, "b", STRING));

        transaction(config, inputSchema);
    }
}
