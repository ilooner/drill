/*
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
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.exec.proto.beans;

public enum SaslStatus implements com.dyuproject.protostuff.EnumLite<SaslStatus>
{
    SASL_UNKNOWN(0),
    SASL_START(1),
    SASL_IN_PROGRESS(2),
    SASL_SUCCESS(3),
    SASL_FAILED(4);
    
    public final int number;
    
    private SaslStatus (int number)
    {
        this.number = number;
    }
    
    public int getNumber()
    {
        return number;
    }
    
    public static SaslStatus valueOf(int number)
    {
        switch(number) 
        {
            case 0: return SASL_UNKNOWN;
            case 1: return SASL_START;
            case 2: return SASL_IN_PROGRESS;
            case 3: return SASL_SUCCESS;
            case 4: return SASL_FAILED;
            default: return null;
        }
    }
}
