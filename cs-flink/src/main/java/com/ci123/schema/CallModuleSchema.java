package com.ci123.schema;

import com.ci123.bean.CallModule;
import jdk.nashorn.internal.codegen.CompilerConstants;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.schema
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/10/31 11:23
 */
public class CallModuleSchema implements ModuleSchema<CallModule> {

    private CallModule callModule ;

    public CallModuleSchema(CallModule callModule){
        this.callModule = callModule ;
    }

    @Override
    public CallModule deserialize(byte[] message) throws IOException {
        String data = message.toString();
        System.out.println(data);
        return callModule ;
    }

    @Override
    public boolean isEndOfStream(Object nextElement) {
        return false;
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(CallModule.class);
    }
}
