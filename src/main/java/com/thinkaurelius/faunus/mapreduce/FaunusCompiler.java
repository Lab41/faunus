package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.formats.FormatTools;
import com.thinkaurelius.faunus.formats.JobConfigurationFormat;
import com.thinkaurelius.faunus.hdfs.NoSideEffectFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusCompiler extends Configured implements Tool {

    enum State {MAPPER, REDUCER, NONE}

    private static final String ARROW = " > ";
    private static final String MAPREDUCE_MAP_OUTPUT_COMPRESS = "mapreduce.map.output.compress";
    private static final String MAPREDUCE_MAP_OUTPUT_COMPRESS_CODEC = "mapreduce.map.output.compress.codec";
    private static final String MAPREDUCE_JOB_JAR = "mapreduce.job.jar";

    public static final String PATH_ENABLED = Tokens.makeNamespace(FaunusCompiler.class) + ".pathEnabled";
    public static final String TESTING = Tokens.makeNamespace(FaunusCompiler.class) + ".testing";
    public static final Logger logger = Logger.getLogger(FaunusCompiler.class);

    private FaunusGraph graph;

    protected final List<Job> jobs = new ArrayList<Job>();

    private State state = State.NONE;

    private static final Class<? extends InputFormat> INTERMEDIATE_INPUT_FORMAT = SequenceFileInputFormat.class;
    private static final Class<? extends OutputFormat> INTERMEDIATE_OUTPUT_FORMAT = SequenceFileOutputFormat.class;

    private boolean pathEnabled = false;

    public FaunusCompiler(final FaunusGraph graph) {
        this.graph = graph;
        this.setConf(new Configuration());
        this.addConfiguration(this.graph.getConf());
    }

    private String makeClassName(final Class klass) {
        return klass.getCanonicalName().replace(klass.getPackage().getName() + ".", "");
    }

    private void addConfiguration(final Configuration configuration) {
        for (final Map.Entry<String, String> entry : configuration) {
            if (entry.getKey().equals(PATH_ENABLED) & Boolean.valueOf(entry.getValue()))
                this.pathEnabled = true;
            //this.getConf().set(entry.getKey() + "-" + this.mapSequenceClasses.size(), entry.getValue());
            this.getConf().set(entry.getKey(), entry.getValue());
        }
    }

    public void addMapReduce(final Class<? extends Mapper> mapper,
                             final Class<? extends Reducer> combiner,
                             final Class<? extends Reducer> reducer,
                             final Class<? extends WritableComparable> mapOutputKey,
                             final Class<? extends WritableComparable> mapOutputValue,
                             final Class<? extends WritableComparable> reduceOutputKey,
                             final Class<? extends WritableComparable> reduceOutputValue,
                             final Configuration configuration) {

        this.addMapReduce(mapper, combiner, reducer, null, mapOutputKey, mapOutputValue, reduceOutputKey, reduceOutputValue, configuration);
    }

    public void addMapReduce(final Class<? extends Mapper> mapper,
                             final Class<? extends Reducer> combiner,
                             final Class<? extends Reducer> reducer,
                             final Class<? extends WritableComparator> comparator,
                             final Class<? extends WritableComparable> mapOutputKey,
                             final Class<? extends WritableComparable> mapOutputValue,
                             final Class<? extends WritableComparable> reduceOutputKey,
                             final Class<? extends WritableComparable> reduceOutputValue,
                             final Configuration configuration) {
        try {
            final Job job;
            if (State.NONE == this.state || State.REDUCER == this.state) {
                job = Job.getInstance(this.getConf());
                job.setJobName(makeClassName(mapper) + ARROW + makeClassName(reducer));
                this.jobs.add(job);
            } else {
                job = this.jobs.get(this.jobs.size() - 1);
                job.setJobName(job.getJobName() + ARROW + makeClassName(mapper) + ARROW + makeClassName(reducer));
            }
            job.setNumReduceTasks(this.getConf().getInt("mapreduce.job.reduces", this.getConf().getInt("mapreduce.tasktracker.reduce.tasks.maximum", 1)));

            ChainMapper.addMapper(job, mapper, NullWritable.class, FaunusVertex.class, mapOutputKey, mapOutputValue, configuration);
            ChainReducer.setReducer(job, reducer, mapOutputKey, mapOutputValue, reduceOutputKey, reduceOutputValue, configuration);
            if (null != comparator)
                job.setSortComparatorClass(comparator);
            if (null != combiner)
                job.setCombinerClass(combiner);
            if (null == job.getConfiguration().get(MAPREDUCE_MAP_OUTPUT_COMPRESS, null))
                job.getConfiguration().setBoolean(MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
            if (null == job.getConfiguration().get(MAPREDUCE_MAP_OUTPUT_COMPRESS_CODEC, null))
                job.getConfiguration().setClass(MAPREDUCE_MAP_OUTPUT_COMPRESS_CODEC, DefaultCodec.class, CompressionCodec.class);
            this.state = State.REDUCER;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void addMap(final Class<? extends Mapper> mapper,
                       final Class<? extends WritableComparable> mapOutputKey,
                       final Class<? extends WritableComparable> mapOutputValue,
                       final Configuration configuration) {

        try {
            final Job job;
            if (State.NONE == this.state) {
                job = Job.getInstance(this.getConf());
                job.setNumReduceTasks(0);
                job.setJobName(makeClassName(mapper));
                this.jobs.add(job);
            } else {
                job = this.jobs.get(this.jobs.size() - 1);
                job.setJobName(job.getJobName() + ARROW + makeClassName(mapper));
            }

            if (State.MAPPER == this.state || State.NONE == this.state) {
                ChainMapper.addMapper(job, mapper, NullWritable.class, FaunusVertex.class, mapOutputKey, mapOutputValue, configuration);
                this.state = State.MAPPER;
            } else {
                ChainReducer.addMapper(job, mapper, NullWritable.class, FaunusVertex.class, mapOutputKey, mapOutputValue, configuration);
                this.state = State.REDUCER;
            }

        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void composeJobs() throws IOException {
        if (this.jobs.size() == 0) {
            return;
        }

        String hadoopFileJar = graph.getConf().get(MAPREDUCE_JOB_JAR, null);
        if (null == hadoopFileJar) {
            if (new File("target/" + Tokens.FAUNUS_JOB_JAR).exists()) {
                hadoopFileJar = "target/" + Tokens.FAUNUS_JOB_JAR;
                logger.warn("Using the developer Faunus job jar: " + hadoopFileJar);
            } else if (new File("../target/" + Tokens.FAUNUS_JOB_JAR).exists()) {
                hadoopFileJar = "../target/" + Tokens.FAUNUS_JOB_JAR;
                logger.warn("Using the developer Faunus job jar: " + hadoopFileJar);
            } else if (new File("lib/" + Tokens.FAUNUS_JOB_JAR).exists()) {
                hadoopFileJar = "lib/" + Tokens.FAUNUS_JOB_JAR;
                logger.warn("Using the distribution Faunus job jar: " + hadoopFileJar);
            } else if (new File("../lib/" + Tokens.FAUNUS_JOB_JAR).exists()) {
                hadoopFileJar = "../lib/" + Tokens.FAUNUS_JOB_JAR;
                logger.warn("Using the distribution Faunus job jar: " + hadoopFileJar);
            } else {
                final String faunusHome = System.getenv(Tokens.FAUNUS_HOME);
                if (null == faunusHome || faunusHome.isEmpty())
                    throw new IllegalStateException("FAUNUS_HOME must be set in order to locate the Faunus Hadoop job jar: " + Tokens.FAUNUS_JOB_JAR);
                if (new File(faunusHome + "/lib/" + Tokens.FAUNUS_JOB_JAR).exists()) {
                    hadoopFileJar = faunusHome + "/lib/" + Tokens.FAUNUS_JOB_JAR;
                    logger.info("Using the distribution Faunus job jar: " + hadoopFileJar);
                }
            }
        } else {
            logger.info("Using the provided Faunus job jar: " + hadoopFileJar);
        }
        if (null == hadoopFileJar)
            throw new IllegalStateException("The Faunus Hadoop job jar could not be found: " + Tokens.FAUNUS_JOB_JAR);

        if (this.pathEnabled)
            logger.warn("Path calculations are enabled for this Faunus job (space and time expensive)");

        final FileSystem hdfs = FileSystem.get(this.graph.getConf());
        final String outputJobPrefix = this.graph.getOutputLocation().toString() + "/" + Tokens.JOB;
        hdfs.mkdirs(this.graph.getOutputLocation());

        //////// CHAINING JOBS TOGETHER

        for (int i = 0; i < this.jobs.size(); i++) {
            final Job job = this.jobs.get(i);
            job.getConfiguration().setBoolean(PATH_ENABLED, this.pathEnabled);
            job.setJar(hadoopFileJar);

            FileOutputFormat.setOutputPath(job, new Path(outputJobPrefix + "-" + i));

            // configure job inputs
            if (i == 0) {
                job.setInputFormatClass(this.graph.getGraphInputFormat());
                if (FileInputFormat.class.isAssignableFrom(this.graph.getGraphInputFormat())) {
                    FileInputFormat.setInputPaths(job, this.graph.getInputLocation());
                    FileInputFormat.setInputPathFilter(job, NoSideEffectFilter.class);
                }
            } else {
                job.setInputFormatClass(INTERMEDIATE_INPUT_FORMAT);
                FileInputFormat.setInputPaths(job, new Path(outputJobPrefix + "-" + (i - 1)));
                FileInputFormat.setInputPathFilter(job, NoSideEffectFilter.class);
            }

            // configure job outputs
            if (i == this.jobs.size() - 1) {
                LazyOutputFormat.setOutputFormatClass(job, this.graph.getGraphOutputFormat());
                MultipleOutputs.addNamedOutput(job, Tokens.SIDEEFFECT, this.graph.getSideEffectOutputFormat(), job.getOutputKeyClass(), job.getOutputKeyClass());
                MultipleOutputs.addNamedOutput(job, Tokens.GRAPH, this.graph.getGraphOutputFormat(), NullWritable.class, FaunusVertex.class);
            } else {
                LazyOutputFormat.setOutputFormatClass(job, INTERMEDIATE_OUTPUT_FORMAT);
                MultipleOutputs.addNamedOutput(job, Tokens.SIDEEFFECT, this.graph.getSideEffectOutputFormat(), job.getOutputKeyClass(), job.getOutputKeyClass());
                MultipleOutputs.addNamedOutput(job, Tokens.GRAPH, INTERMEDIATE_OUTPUT_FORMAT, NullWritable.class, FaunusVertex.class);
            }
        }
    }

    public List<Job> getJobs() {
        return this.jobs;
    }

    public int run(final String[] args) throws Exception {
        String script = null;
        boolean showHeader = true;

        if (args.length == 2) {
            script = args[0];
            showHeader = Boolean.valueOf(args[1]);
        }

        if (showHeader) {
            logger.info("Faunus: Graph Analytics Engine");
            logger.info("        ,");
            logger.info("    ,   |\\ ,__");
            logger.info("    |\\   \\/   `\\");
            logger.info("    \\ `-.:.     `\\");
            logger.info("     `-.__ `\\/\\/\\|");
            logger.info("        / `'/ () \\");
            logger.info("      .'   /\\     )");
            logger.info("   .-'  .'| \\  \\__");
            logger.info(" .'  __(  \\  '`(()");
            logger.info("/_.'`  `.  |    )(");
            logger.info("         \\ |");
            logger.info("          |/");
        }

        if (null != script && !script.isEmpty()) {
            logger.info("Generating job chain: " + script);
        }

        this.composeJobs();
        logger.info("Compiled to " + this.jobs.size() + " MapReduce job(s)");

        FaunusJobControl faunusJobControl = new FaunusJobControl(graph, jobs);
        faunusJobControl.run();

        if (faunusJobControl.getFailedJobs().size() == 0) {
            return 0;
        } else {
            return -1;
        }
    }
}