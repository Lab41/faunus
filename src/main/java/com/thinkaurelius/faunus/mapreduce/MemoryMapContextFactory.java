package com.thinkaurelius.faunus.mapreduce;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author kramachandran
 */
public class MemoryMapContextFactory {


    private static Logger logger = LoggerFactory.getLogger(MemoryMapContextFactory.class);
    private static final boolean useV21;


    static
    {
        boolean v21 = true;
        final String PACKAGE = "org.apache.hadoop.mapreduce";
        try {
            Class.forName("org.apache.hadoop.mapreduce.task.MapContextImpl");
        } catch (ClassNotFoundException cnfe) {
            v21 = false;
        }
        useV21 = v21;


    }

    /**
     * This interceptor is designed to intercept calls to Mapper.Context.
     * Where it intercepts functions implemented by MemoryMapper.MemoryMapContext it calls those functions on an
     * instance of MemoryMapContext.
     *
     */
    public static class ContextInterceptor implements MethodInterceptor{

        private Logger logger = LoggerFactory.getLogger(ContextInterceptor.class);
        private MemoryMapper.MemoryMapContext memoryMapContext;
        private MapContext regularMapContext;

        public ContextInterceptor(MemoryMapper.MemoryMapContext memoryMapContext, MapContext regularMapContext)
        {

            this.memoryMapContext = memoryMapContext;
            this.regularMapContext = regularMapContext;

        }

       /**
        * Compares this <code>Method</code> against the specified object.  Returns
        * true if the objects are the same.  Two <code>Methods</code> are the same if
        * they have the same name and formal parameter types and return type.
        */
        public boolean methodEquals(Method method1, Method method2) {
            if (method1 != null && method2 != null &&
                    method1 instanceof Method && method2 instanceof  Method)
            {

                if ((method1.getName() == method2.getName())) {
                    if (!method1.getReturnType().equals(method2.getReturnType()))
                        return false;
                    Class[] params1 = method1.getParameterTypes();
                    Class[] params2 = method2.getParameterTypes();
                    if (params1.length == params2.length) {
                        for (int i = 0; i < params1.length; i++) {
                            if (params1[i] != params2[i])
                                return false;
                        }
                        return true;
                    }
                }
            }
            return false;
        }
        public Object intercept(Object o,
                                Method method,
                                Object[] objects,
                                MethodProxy methodProxy) throws Throwable
        {

            logger.debug("Intercepting call : " + method.getName() + "for class " + o.getClass().getName());
            for(Method memoryMapContextMethod :memoryMapContext.getClass().getDeclaredMethods())
            {

                //If it is a method implemented by the object itself the we
                //call the real object
                if(methodEquals(memoryMapContextMethod,method))
                {
                    logger.debug("call routed to memoryMapContext");
                    return memoryMapContextMethod.invoke(memoryMapContext, objects);
                }
            }


            //doesn't match any of the memoryMapContext so we will send this to the delgate

            //TODO figure out if methodProxy can be used here instead of method.invoke.
            // If we can use methodProxy then this call might be faster?
            logger.debug("call routed to delegate");
            for(Method regularMapContextMethod :regularMapContext.getClass().getDeclaredMethods())
            {

                //If it is a method implemented by the object itself the we
                //call the real object
                if(methodEquals(regularMapContextMethod,method))
                {
                    logger.debug("call routed to memoryMapContext");
                    return regularMapContextMethod.invoke(regularMapContext, objects);
                }
            }

            throw new RuntimeException("Method Not Found");
        }

    }

    public static boolean isHadoop2()
    {
         return false;
    }

    public static <KEYIN, VALUEIN, KEYOUT, VALUEOUT> MemoryMapper.MemoryMapContext createMemoryMapperContext(MemoryMapper mapper, Mapper.Context context)
            throws IOException, InterruptedException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
    {
        boolean isV2 = false;
        Enhancer enhancer = new Enhancer();

        MemoryMapper.MemoryMapContext memoryMapContext = mapper. new MemoryMapContextImpl(context);
        ContextInterceptor contextInterceptor;




        Class[] v1ArgTypes = { Mapper.class,
                Configuration.class,
                TaskAttemptID.class,
                RecordReader.class,
                RecordWriter.class,
                OutputCommitter.class,
                StatusReporter.class,
                InputSplit.class};

        Object[] v1Arguments = {mapper,
                context.getConfiguration(),
                context.getTaskAttemptID() == null ? new TaskAttemptID() : context.getTaskAttemptID(),
                null,
                null,
                context.getOutputCommitter(),
                null,
                context.getInputSplit()};

        Class[] v2ArgTypes = { Configuration.class,
                TaskAttemptID.class,
                RecordReader.class,
                RecordWriter.class,
                OutputCommitter.class,
                StatusReporter.class,
                InputSplit.class};

        Object[] v2Arguments = {context.getConfiguration(),
                context.getTaskAttemptID() == null ? new TaskAttemptID() : context.getTaskAttemptID(),
                null,
                null,
                context.getOutputCommitter(),
                null,
                context.getInputSplit()};

        if(useV21)
        {

            Class clazzMapContextImpl = Class.forName("org.apache.hadoop.mapreduce.task.MapContextImpl");
            Constructor implConstructor = clazzMapContextImpl.getConstructor(v2ArgTypes);
            Object mapContextImpl = implConstructor.newInstance(v2Arguments);

            contextInterceptor = new ContextInterceptor(memoryMapContext, (MapContext)mapContextImpl);
            enhancer.setCallback(contextInterceptor);

            Class classMapContextInterface = Class.forName("org.apache.hadoop.mapreduce.MapContext");
            Class[] interfaces=   {classMapContextInterface, MemoryMapper.MemoryMapContext.class};
            enhancer.setInterfaces(interfaces);


            enhancer.setSuperclass(Mapper.Context.class);
            Class[] mapperContextParameterTypes = {Mapper.class};
            Object[]  mapperContextArguments = {mapper};
            return (MemoryMapper.MemoryMapContext) enhancer.create(mapperContextParameterTypes, mapperContextArguments);
        }
        else

        {
            Class clazzMapContextImpl = Mapper.Context.class;
            Constructor implConstructor = clazzMapContextImpl.getConstructor(v1ArgTypes);
            Object mapContextImpl = implConstructor.newInstance(v1Arguments);


            contextInterceptor = new ContextInterceptor(memoryMapContext, (MapContext) mapContextImpl);
            enhancer.setCallback(contextInterceptor);

            Class[] interfaces = {MemoryMapper.MemoryMapContext.class};
            enhancer.setInterfaces(interfaces);
            enhancer.setSuperclass(Mapper.Context.class);
            return (MemoryMapper.MemoryMapContext) enhancer.create(v1ArgTypes, v1Arguments);
        }
    }
}
