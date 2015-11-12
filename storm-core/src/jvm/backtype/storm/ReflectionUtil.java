/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.util.*;

public class ReflectionUtil {

    private static final Set<Class> WRAPPER_TYPES = new HashSet<Class>(){{
        add(Boolean.class);
        add(Character.class);
        add(Byte.class);
        add(Short.class);
        add(Integer.class);
        add(Long.class);
        add(Float.class);
        add(Double.class);
        add(Void.class);
        add(String.class);
    }};

    public static boolean isWrapperType(Class clazz)
    {
        return WRAPPER_TYPES.contains(clazz);
    }

    public static boolean isCollectionType(Class clazz) {
        return Collection.class.isAssignableFrom(clazz);
    }

    public static boolean isMapType(Class clazz) {
        return Map.class.isAssignableFrom(clazz);
    }


    public static Map<String, String> getFieldValues(Object instance, boolean prependClassName) throws IllegalAccessException {
        Class clazz = instance.getClass();
        Map<String, String> output = new HashMap<>();
        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            Field[] fields = c.getDeclaredFields();
            for (Field field : fields) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    continue;
                }

                String key;
                if(prependClassName) {
                    key = String.format("%s.%s", clazz.getSimpleName(), field.getName());
                } else {
                    key = field.getName();
                }

                boolean accessible = field.isAccessible();
                if (!accessible) {
                    field.setAccessible(true);
                }
                Object fieldVal = field.get(instance);
                if(fieldVal == null) {
                    output.put(key, null);
                } else if(fieldVal.getClass().isPrimitive() || isWrapperType(fieldVal.getClass())) {
                    output.put(key, toString(fieldVal, true));
                } else if(isMapType(fieldVal.getClass())) {
                    //TODO: check if it makes more sense to just stick to json like structure instead of a flatten output.
                    Map map = (Map) fieldVal;
                    for (Object entry : map.entrySet()) {
                        Object mapKey =  ((Map.Entry) entry).getKey();
                        Object mapVal =  ((Map.Entry) entry).getValue();

                        String keyStr = getString(mapKey, false);
                        String valStr = getString(mapVal, true);
                        output.put(String.format("%s.%s", key, keyStr), valStr);
                    }
                } else if(isCollectionType(fieldVal.getClass())) {
                    //TODO check if it makes more sense to just stick to json like structure instead of a flatten output.
                    Collection collection =   (Collection) fieldVal;
                    String outStr = "";
                    for (Object o : collection) {
                        outStr +=  getString(o, true)+ ",";
                    }
                    if (outStr.length() > 0) {
                        outStr = outStr.substring(0, outStr.length() -1);
                    }
                    output.put(key, String.format("%s", outStr).toString());
                } else {
                    Map<String, String> nestedFieldValues = getFieldValues(fieldVal, false);
                    for(Map.Entry<String, String> entry: nestedFieldValues.entrySet()) {
                        output.put(String.format("%s.%s", key, entry.getKey()), entry.getValue());
                    }
                }
                if (!accessible) {
                    field.setAccessible(false);
                }
            }
        }
        return output;
    }

    private static String getString(Object instance, boolean wrapWithQuote) throws IllegalAccessException {
        if (instance == null) {
            return null;
        } else if(instance.getClass().isPrimitive() || isWrapperType(instance.getClass())) {
           return toString(instance, wrapWithQuote);
        } else {
            return getString(getFieldValues(instance, false), wrapWithQuote);
        }
    }

    private static String getString(Map<String, String> flattenFields, boolean wrapWithQuote) {
        String outStr = "";
        if(flattenFields != null && !flattenFields.isEmpty()) {
            if(wrapWithQuote) {
                outStr += "\"" + Joiner.on(",").join(flattenFields.entrySet()) + "\",";
            } else {
                outStr += Joiner.on(",").join(flattenFields.entrySet()) + ",";
            }
        }
        if (outStr.length() > 0) {
            outStr = outStr.substring(0, outStr.length()-1);
        }
        return outStr;
    }

    private static final String toString(Object instance, boolean wrapWithQuote) {
        if(instance instanceof String)
            if(wrapWithQuote)
                return "\"" + instance + "\"";
            else
                return instance.toString();
        else
            return instance.toString();
    }

    public static void main(String[] args) throws Exception {
        TestInfo testInfo = new TestInfo(1, "bval", 2l);
        System.out.println(ReflectionUtil.getFieldValues(testInfo, true));
    }
}

class TestInfo {
    private int a;
    private String b;
    public Long c;
    public BrokerEndPoint testEp =  new BrokerEndPoint("host",1000);
    protected List<BrokerEndPoint> endPoints = Lists.newArrayList(new BrokerEndPoint("host-1", 8080), new BrokerEndPoint("host-2", 9090));
    private Map<String, BrokerEndPoint> hostToEndPoints;
    private List<String> someStrs = Lists.newArrayList("hi", "hello");

    public TestInfo(int a, String b, Long c) {
        this.a = a;
        this.b = b;
        this.hostToEndPoints = new HashMap<>();
        this.hostToEndPoints.put("host-1", new BrokerEndPoint("host-1", 8080));
        this.hostToEndPoints.put("host-2", new BrokerEndPoint("host-2", 9090));
    }
}

class BrokerEndPoint {
  private String host;
  private int port;

    public BrokerEndPoint(String host, int port) {
        this.host = host;
        this.port = port;
    }
}
