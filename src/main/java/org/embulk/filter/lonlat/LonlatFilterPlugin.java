package org.embulk.filter.lonlat;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.LONG;

public class LonlatFilterPlugin
        implements FilterPlugin
{
    public static final String TYPE_DEGREE = "DEG";
    public static final String TYPE_MILLISEC = "MSEC";

    static Schema buildOutputSchema(PluginTask task, Schema inputSchema)
    {
        List<LonLatColumnTask> latColumns = task.getLatColumns();
        List<LonLatColumnTask> lonColumns = task.getLonColumns();

        // Automatically get column type from inputSchema for columns and dropColumns
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;

        for (Column column : inputSchema.getColumns()) {

            Column outputColumn = new Column(i, column.getName(), column.getType());

            for (LonLatColumnTask latColumn : latColumns) {
                if (!column.getName().equals(latColumn.getName())) {
                    continue;
                }
                outputColumn = (latColumn.getFormat().equals(TYPE_DEGREE)) ?
                        new Column(i, column.getName(), DOUBLE) : new Column(i, column.getName(), LONG);
            }
            for (LonLatColumnTask lonColumn : lonColumns) {
                if (!column.getName().equals(lonColumn.getName())) {
                    continue;
                }
                outputColumn = (lonColumn.getFormat().equals(TYPE_DEGREE)) ?
                        new Column(i, column.getName(), DOUBLE) : new Column(i, column.getName(), LONG);
            }
            builder.add(outputColumn);
            i++;
        }
        return new Schema(builder.build());
    }

    static Map<String, LonLatColumnTask> getTaskColumnMap(
            List<LonLatColumnTask> latColumnTasks,
            List<LonLatColumnTask> lonColumnTasks)
    {
        Map<String, LonLatColumnTask> m = new HashMap<>();
        for (LonLatColumnTask columnTask : latColumnTasks) {
            m.put(columnTask.getName(), columnTask);
        }
        for (LonLatColumnTask columnTask : lonColumnTasks) {
            m.put(columnTask.getName(), columnTask);
        }
        return m;
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        for (LonLatColumnTask latColumnTask : task.getLatColumns()) {
            // throws exception if the column name does not exist
            Column column = inputSchema.lookupColumn(latColumnTask.getName());
        }
        for (LonLatColumnTask lonColumnTask : task.getLonColumns()) {
            // throws exception if the column name does not exist
            inputSchema.lookupColumn(lonColumnTask.getName());
        }
//        Schema outputSchema = inputSchema;
        Schema outputSchema = buildOutputSchema(task, inputSchema);
        control.run(task.dump(), outputSchema);
    }

    private long convertDeg2Dms(double deg)
    {
        Double d = (deg * 60 * 60 * 1000);
        return d.longValue();
    }

    private double convertMsec2Deg(long mSec)
    {
        return mSec / (60 * 60 * 1000);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        final Map<String, LonLatColumnTask> columnTaskMap = getTaskColumnMap(task.getLatColumns(), task.getLonColumns());

//        throw new UnsupportedOperationException("LonlatFilterPlugin.open method is not implemented yet");
        return new PageOutput()
        {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(
                    Exec.getBufferAllocator(), outputSchema, output);
            private ColumnVisitorImpl visitor = new ColumnVisitorImpl(builder);

            @Override
            public void add(Page page)
            {
                reader.setPage(page);

                while (reader.nextRecord()) {
                    for (Column column : inputSchema.getColumns()) {
                        String colName = column.getName();
                        LonLatColumnTask colTask = columnTaskMap.get(colName);

                        // do nothing
                        if (colTask == null) {
                            column.visit(visitor);
                            continue;
                        }

                        // do convert
                        if (colTask.getFormat().or("").equals(TYPE_MILLISEC)) {
                            double raw = reader.getDouble(column);
                            builder.setLong(column, convertDeg2Dms(raw));
                        }
                        else if (colTask.getFormat().or("").equals(TYPE_DEGREE)) {
                            long raw = reader.getLong(column);
                            builder.setDouble(column, convertMsec2Deg(raw));
                        }
                    }
                    //visitcolumns
                    builder.addRecord();
                }
            }

            @Override
            public void finish()
            {
                builder.finish();
            }

            @Override
            public void close()
            {
                builder.close();
            }

            class ColumnVisitorImpl
                    implements ColumnVisitor
            {
                private final PageBuilder builder;

                ColumnVisitorImpl(PageBuilder builder)
                {
                    this.builder = builder;
                }

                @Override
                public void booleanColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setBoolean(outputColumn, reader.getBoolean(outputColumn));
                    }
                }

                @Override
                public void longColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setLong(outputColumn, reader.getLong(outputColumn));
                    }
                }

                @Override
                public void doubleColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setDouble(outputColumn, reader.getDouble(outputColumn));
                    }
                }

                @Override
                public void stringColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setString(outputColumn, reader.getString(outputColumn));
                    }
                }

                @Override
                public void timestampColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setTimestamp(outputColumn, reader.getTimestamp(outputColumn));
                    }
                }

                @Override
                public void jsonColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    }
                    else {
                        builder.setJson(outputColumn, reader.getJson(outputColumn));
                    }
                }
            }
        };
    }

    public interface PluginTask
            extends Task
    {

        @Config("lon_columns")
        public List<LonLatColumnTask> getLonColumns();

        @Config("lat_columns")
        public List<LonLatColumnTask> getLatColumns();
    }

    public interface LonLatColumnTask
            extends Task
    {
        //        {name: lat, type: lat, format: DMS, datum: WGS84}
        @Config("name")
        String getName();

        @Config("type")
        String getType();

        @Config("format")
        @ConfigDefault("DEG")
        Optional<String> getFormat();

        @Config("datum")
        @ConfigDefault("WGS84")
        Optional<String> getDatum();
    }
}
