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
import java.util.Map;

/**
 * @author kramachandran
 */
public class MemoryMapContextFactory {


    private static Logger logger = LoggerFactory.getLogger(MemoryMapContextFactory.class);

    /**
     * This interceptor is designed to intercept calls to Mapper.Context.
     * Where it intercepts functions implemented by MemoryMapper.MemoryMapContext it calls those functions on an
     * instance of MemoryMapContext.
     *
     *
     *
     *
     */
    public static class ContextInterceptor implements MethodInterceptor{

        private Logger logger = LoggerFactory.getLogger(ContextInterceptor.class);
        private MemoryMapper.MemoryMapContext memoryMapContext;

        private Map<Method, Object> memoryMapContextMethods;
        public ContextInterceptor(MemoryMapper.MemoryMapContext memoryMapContext)
        {
            this.memoryMapContext = memoryMapContext;

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
		/* Avoid unnecessary cloning */
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
            logger.debug("call rounted to delgate");
                return methodProxy.invokeSuper(o, objects);


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
        ContextInterceptor contextInterceptor = new ContextInterceptor(memoryMapContext);
        enhancer.setCallback(contextInterceptor);

        Class[] interfaces = {MemoryMapper.MemoryMapContext.class};
        enhancer.setInterfaces(interfaces);

        Class[] argTypes = { Mapper.class,
                Configuration.class,
                TaskAttemptID.class,
                RecordReader.class,
                RecordWriter.class,
                OutputCommitter.class,
                StatusReporter.class,
                InputSplit.class};

        Object[] arguments = {mapper,
                context.getConfiguration(),
                context.getTaskAttemptID() == null ? new TaskAttemptID() : context.getTaskAttemptID(),
                null,
                null,
                context.getOutputCommitter(),
                null,
                context.getInputSplit()};

        if(isV2)
        {
            //If we are dealing with Hadoop 2 the we need to set the super class to be
            //org.apache.hadoop.mapreduce.task.MapContextImpl
            //In Hadoop Mapper.Context implements org.apache.hadoop.mapreduce.MapContext
            //so we don't have to add that as interface.
            Class clazzMapContextImpl = Class.forName("org.apache.hadoop.mapreduce.task.MapContextImpl");
            enhancer.setSuperclass(clazzMapContextImpl);

            return (MemoryMapper.MemoryMapContext) enhancer.create(argTypes, arguments);
        }
        else
        {
            enhancer.setSuperclass(Mapper.Context.class);
            return (MemoryMapper.MemoryMapContext) enhancer.create(argTypes, arguments);
        }
    }
}
